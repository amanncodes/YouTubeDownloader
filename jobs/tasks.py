from __future__ import annotations

import logging
from celery import shared_task
from django.db import transaction

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

# Infrastructure (worker-local singletons)
PROXY_POOL = ProxyPool(
    proxies=[
        # TODO: Replace with DB-backed proxies later
        Proxy("http://1.2.3.4:8000", "datacenter"),
        Proxy("http://5.6.7.8:8000", "residential"),
    ],
    max_failures=3,
    cooldown_seconds=300,
)

COOKIE_VAULT = CookieVault(
    cookie_files=[
        # TODO: Replace with real cookies.txt paths
        "cookies/a.txt",
        "cookies/b.txt",
    ],
    max_failures=2,
    cooldown_seconds=600,
)

# COLLECTION TASK (PLAYLIST / CHANNEL FAN-OUT)
@shared_task(bind=True, acks_late=True)
def process_collection_job(self, parent_job_id: str):
    """
    Fan-out a PLAYLIST or CHANNEL job into child VIDEO jobs.
    """
    try:
        parent = DownloadJob.objects.get(id=parent_job_id)
    except DownloadJob.DoesNotExist:
        logger.warning("Parent job %s does not exist", parent_job_id)
        return

    if parent.status != JobStatus.PENDING:
        logger.info("Parent job %s already processed", parent.id)
        return

    parent.update_status(JobStatus.DISCOVERING)

    try:
        video_urls = DiscoveryEngine.extract_video_urls(parent.source_url)
    except Exception as exc:
        parent.mark_failed(
            error_code="DISCOVERY_FAILED",
            error_message=str(exc),
        )
        return

    if not video_urls:
        parent.mark_failed(
            error_code="EMPTY_COLLECTION",
            error_message="No videos found",
        )
        return

    children = []
    for url in video_urls:
        child = DownloadJob.objects.create(
            job_type="VIDEO",
            source_url=url,
            parent=parent,
        )
        children.append(child)

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
@shared_task(bind=True, acks_late=True)
def process_download_job(self, job_id: str):
    """
    Execute a DownloadJob safely with proxy + cookie rotation.

    This task is:
    - idempotent
    - retry-safe
    - crash-safe
    """

    try:
        job = DownloadJob.objects.select_for_update().get(id=job_id)
    except DownloadJob.DoesNotExist:
        logger.warning("Job %s does not exist", job_id)
        return

    if job.status in {JobStatus.COMPLETED, JobStatus.FAILED}:
        logger.info("Job %s already finished (%s)", job.id, job.status)
        return

    proxy: str | None = None
    cookie_path = None

    try:
        # Start job
        with transaction.atomic():
            job.mark_started()
            job.update_status(JobStatus.FETCHING_METADATA)

        # Acquire identity (optional)
        proxy = PROXY_POOL.get_proxy(proxy_type="datacenter")
        if proxy is None:
            logger.warning("No healthy proxy available, using direct connection")

        cookie_path = COOKIE_VAULT.get_cookie()

        logger.info(
            "Job %s using proxy=%s cookie=%s",
            job.id,
            proxy,
            cookie_path,
        )

        engine = YtDlpEngine(
            work_dir="tmp_downloads",
            proxy=proxy,
            cookie_file=str(cookie_path) if cookie_path else None,
        )

        # Metadata extraction
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

        # Download with progress reporting
        def on_progress(percent: float):
            job.update_progress(percent)
            if job.parent:
                job.parent.recompute_parent_progress()

        job.update_status(JobStatus.DOWNLOADING)

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

        if job.parent:
            job.parent.recompute_parent_progress()

        logger.info("Job %s completed successfully", job.id)

    # Controlled failures
    except MetadataExtractionError as exc:
        reason = str(exc)

        logger.warning(
            "Metadata extraction issue for job %s: %s",
            job.id,
            reason,
        )

        if reason == "COOKIE_INVALID":
            if cookie_path:
                COOKIE_VAULT.report_failure(cookie_path)
            raise self.retry(exc=exc)

        if reason == "PROXY_FAILED":
            if proxy:
                PROXY_POOL.report_failure(proxy)
            raise self.retry(exc=exc)

        job.mark_failed(
            error_code="METADATA_EXTRACTION_FAILED",
            error_message=reason,
        )

    except DownloadError as exc:
        logger.exception("Download failed for job %s", job.id)

        if proxy:
            PROXY_POOL.report_failure(proxy)
        if cookie_path:
            COOKIE_VAULT.report_failure(cookie_path)

        job.increment_retry()

        if job.can_retry():
            raise self.retry(exc=exc)
        else:
            job.mark_failed(
                error_code="DOWNLOAD_FAILED",
                error_message=str(exc),
            )

    # Unexpected failures
    except Exception as exc:
        logger.exception("Unexpected error for job %s", job.id)

        if proxy:
            PROXY_POOL.report_failure(proxy)
        if cookie_path:
            COOKIE_VAULT.report_failure(cookie_path)

        job.mark_failed(
            error_code="UNEXPECTED_ERROR",
            error_message=str(exc),
        )
