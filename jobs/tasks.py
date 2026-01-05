from __future__ import annotations

import logging
from celery import shared_task
from django.db import transaction

from jobs.models import DownloadJob, JobStatus
from media.models import VideoMetadata
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

# Celery task
@shared_task(
    bind=True,
    acks_late=True,
    autoretry_for=(DownloadError,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
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

    # Guard against duplicate execution
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

        # Acquire identity
        proxy = PROXY_POOL.get_proxy(proxy_type="datacenter")
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

        # Persist metadata (idempotent)
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

        # Download with progress hook
        def on_progress(percent: float):
            job.update_progress(percent)
            if job.parent:
                job.parent.recompute_parent_progress()

        job.update_status(JobStatus.DOWNLOADING)

        engine.download(
            job.source_url,
            progress_callback=on_progress,
        )

        # Success reporting
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
        logger.exception("Metadata extraction failed for job %s", job.id)

        if proxy:
            PROXY_POOL.report_failure(proxy)
        if cookie_path:
            COOKIE_VAULT.report_failure(cookie_path)

        job.mark_failed(
            error_code="METADATA_EXTRACTION_FAILED",
            error_message=str(exc),
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
