from __future__ import annotations

import logging
from typing import Optional

from celery import shared_task
from django.db import transaction

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from jobs.models import DownloadJob, JobStatus
from media.models import VideoMetadata
from media_engine.discovery import DiscoveryEngine
from media_engine.yt_dlp_wrapper import (
    YtDlpEngine,
    MetadataExtractionError,
    DownloadError,
)
from infrastructure.proxy_pool import Proxy, ProxyPool
from infrastructure.cookie_vault import CookieVault

logger = logging.getLogger(__name__)
channel_layer = get_channel_layer()

# Helpers
def sanitize_error_message(message: Optional[str], max_length: int = 200) -> str:
    """
    Sanitize error messages before exposing them to WebSocket clients.
    Prevents leaking stack traces, cookies, proxies, or internal details.
    """
    if not message:
        return "An unknown error occurred."

    clean = " ".join(str(message).split())
    return clean[:max_length]


def get_retry_metadata(job: DownloadJob) -> dict:
    """
    Build retry-related metadata for WebSocket payloads.
    """
    return {
        "can_retry": job.can_retry(),
        "retry_count": job.retry_count,
        "max_retries": job.MAX_RETRIES,
        # Hint for UI – approximate Celery backoff
        "retry_after_seconds": (2 ** job.retry_count)
        if job.can_retry()
        else None,
    }


def is_cancelled(job_id: str) -> bool:
    return (
        DownloadJob.objects
        .filter(id=job_id, status=JobStatus.CANCELLED)
        .exists()
    )

# Infrastructure (worker-local singletons)
PROXY_POOL = ProxyPool(
    proxies=[
        Proxy("http://1.2.3.4:8000", "datacenter"),
        Proxy("http://5.6.7.8:8000", "residential"),
    ],
    max_failures=3,
    cooldown_seconds=300,
)

COOKIE_VAULT = CookieVault(
    cookie_files=[
        "cookies/a.txt",
        "cookies/b.txt",
    ],
    max_failures=2,
    cooldown_seconds=600,
)

# COLLECTION TASK (playlist / channel fan-out)
@shared_task(bind=True, acks_late=True)
def process_collection_job(self, parent_job_id: str):
    try:
        parent = DownloadJob.objects.get(id=parent_job_id)
    except DownloadJob.DoesNotExist:
        logger.warning("Parent job %s does not exist", parent_job_id)
        return

    if parent.status != JobStatus.PENDING:
        return

    parent.update_status(JobStatus.DISCOVERING)

    try:
        video_urls = DiscoveryEngine.extract_video_urls(parent.source_url)
    except Exception as exc:
        parent.mark_failed(
            error_code="DISCOVERY_FAILED",
            error_message=sanitize_error_message(str(exc)),
        )
        return

    if not video_urls:
        parent.mark_failed(
            error_code="EMPTY_COLLECTION",
            error_message="No videos found",
        )
        return

    children = [
        DownloadJob.objects.create(
            job_type="VIDEO",
            source_url=url,
            parent=parent,
        )
        for url in video_urls
    ]

    parent.total_children = len(children)
    parent.update_status(JobStatus.DOWNLOADING)
    parent.save(update_fields=["total_children", "status"])

    for child in children:
        process_download_job.delay(str(child.id))

    logger.info(
        "Parent job %s spawned %d child jobs",
        parent.id,
        len(children),
    )

# VIDEO DOWNLOAD TASK
@shared_task(
    bind=True,
    acks_late=True,
    autoretry_for=(DownloadError,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def process_download_job(self, job_id: str):
    try:
        with transaction.atomic():
            job = (
                DownloadJob.objects
                .select_for_update()
                .get(id=job_id)
            )

            if job.status in {
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
            }:
                return

            job.mark_started()
            job.update_status(JobStatus.FETCHING_METADATA)

    except DownloadJob.DoesNotExist:
        logger.warning("Job %s does not exist", job_id)
        return

    # Hard cancellation before doing any work
    if is_cancelled(job.id):
        logger.info("Job %s cancelled before execution", job.id)
        return

    proxy = None
    cookie_path = None

    try:
        # Acquire identity
        proxy = PROXY_POOL.get_proxy(proxy_type="datacenter")
        cookie_path = COOKIE_VAULT.get_cookie()

        engine = YtDlpEngine(
            work_dir="tmp_downloads",
            proxy=proxy,
            cookie_file=str(cookie_path) if cookie_path else None,
        )

        # Metadata
        metadata = engine.extract_metadata(job.source_url)
        youtube_id = metadata.get("id")

        VideoMetadata.objects.update_or_create(
            youtube_id=youtube_id,
            defaults={
                "job": job,
                "title": metadata.get("title"),
                "channel_id": metadata.get("channel_id"),
                "channel_name": metadata.get("uploader"),
                "duration_seconds": metadata.get("duration"),
                "upload_date": metadata.get("upload_date"),
                "view_count": metadata.get("view_count"),
                "like_count": metadata.get("like_count"),
                "raw_metadata": metadata,
            },
        )

        job.update_status(JobStatus.DOWNLOADING)

        # Progress hook
        def on_progress(percent: float):
            if is_cancelled(job.id):
                logger.warning(
                    "Job %s cancelled during download",
                    job.id,
                )
                raise DownloadError("JOB_CANCELLED")

            job.update_progress(percent)

            async_to_sync(channel_layer.group_send)(
                f"job_{job.id}",
                {
                    "type": "job_progress",
                    "data": {
                        "job_id": str(job.id),
                        "status": JobStatus.DOWNLOADING,
                        "progress": percent,
                    },
                },
            )

            if job.parent:
                job.parent.recompute_parent_progress()

        # Download
        engine.download(
            job.source_url,
            progress_callback=on_progress,
        )

        # Success handling
        if proxy:
            PROXY_POOL.report_success(proxy)
        if cookie_path:
            COOKIE_VAULT.report_success(cookie_path)

        job.mark_completed()

        async_to_sync(channel_layer.group_send)(
            f"job_{job.id}",
            {
                "type": "job_progress",
                "data": {
                    "job_id": str(job.id),
                    "status": JobStatus.COMPLETED,
                    "progress": 100.0,
                    "terminal": True,
                },
            },
        )

        if job.parent:
            job.parent.recompute_parent_progress()

    # Controlled failures
    except MetadataExtractionError as exc:
        reason = str(exc)
        error_code = "METADATA_EXTRACTION_FAILED"
        error_message = sanitize_error_message(reason)
        retry_meta = get_retry_metadata(job)

        if reason == "PROXY_FAILED" and proxy:
            PROXY_POOL.report_failure(proxy)
            raise self.retry(exc=exc)

        if reason == "COOKIE_INVALID" and cookie_path:
            COOKIE_VAULT.report_failure(cookie_path)
            raise self.retry(exc=exc)

        async_to_sync(channel_layer.group_send)(
            f"job_{job.id}",
            {
                "type": "job_progress",
                "data": {
                    "job_id": str(job.id),
                    "status": JobStatus.FAILED,
                    "progress": job.progress_percentage,
                    "terminal": True,
                    "error_code": error_code,
                    "error_message": error_message,
                    **retry_meta,
                },
            },
        )

        job.mark_failed(
            error_code=error_code,
            error_message=error_message,
        )

    except DownloadError as exc:
        # Cancellation is treated as a clean terminal state
        if str(exc) == "JOB_CANCELLED":
            logger.info("Job %s cancelled cleanly", job.id)

            async_to_sync(channel_layer.group_send)(
                f"job_{job.id}",
                {
                    "type": "job_progress",
                    "data": {
                        "job_id": str(job.id),
                        "status": JobStatus.CANCELLED,
                        "progress": job.progress_percentage,
                        "terminal": True,
                    },
                },
            )
            return

        # otherwise handled by autoretry_for
        raise

    # Unexpected failures
    except Exception as exc:
        logger.exception("Unexpected failure for job %s", job.id)

        error_code = "UNEXPECTED_ERROR"
        error_message = sanitize_error_message(str(exc))
        retry_meta = get_retry_metadata(job)

        if proxy:
            PROXY_POOL.report_failure(proxy)
        if cookie_path:
            COOKIE_VAULT.report_failure(cookie_path)

        async_to_sync(channel_layer.group_send)(
            f"job_{job.id}",
            {
                "type": "job_progress",
                "data": {
                    "job_id": str(job.id),
                    "status": JobStatus.FAILED,
                    "progress": job.progress_percentage,
                    "terminal": True,
                    "error_code": error_code,
                    "error_message": error_message,
                    **retry_meta,
                },
            },
        )

        job.mark_failed(
            error_code=error_code,
            error_message=error_message,
        )
