from __future__ import annotations

import logging
from celery import shared_task
from django.db import transaction

from jobs.models import DownloadJob, JobStatus
from media_engine.yt_dlp_wrapper import (
    YtDlpEngine,
    MetadataExtractionError,
    DownloadError,
)

logger = logging.getLogger(__name__)


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
    Execute a DownloadJob safely.

    This task is idempotent and retry-safe.
    """
    try:
        job = DownloadJob.objects.select_for_update().get(id=job_id)
    except DownloadJob.DoesNotExist:
        logger.warning("Job %s no longer exists", job_id)
        return

    # Guard against duplicate execution
    if job.status in {JobStatus.COMPLETED, JobStatus.FAILED}:
        logger.info("Job %s already terminal (%s)", job.id, job.status)
        return

    try:
        with transaction.atomic():
            job.update_status(JobStatus.FETCHING_METADATA)
            job.mark_started()

        engine = YtDlpEngine(
            work_dir="tmp_downloads",
        )


        # Metadata extraction
        metadata = engine.extract_metadata(job.source_url)

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


        # Finalize
        job.mark_completed()

        if job.parent:
            job.parent.recompute_parent_progress()

        logger.info("Job %s completed successfully", job.id)

    except MetadataExtractionError as exc:
        logger.exception("Metadata extraction failed for job %s", job.id)
        job.mark_failed(
            error_code="METADATA_EXTRACTION_FAILED",
            error_message=str(exc),
        )

    except DownloadError as exc:
        logger.exception("Download failed for job %s", job.id)

        job.increment_retry()

        if job.can_retry():
            raise self.retry(exc=exc)
        else:
            job.mark_failed(
                error_code="DOWNLOAD_FAILED",
                error_message=str(exc),
            )

    except Exception as exc:
        logger.exception("Unexpected error for job %s", job.id)
        job.mark_failed(
            error_code="UNEXPECTED_ERROR",
            error_message=str(exc),
        )
