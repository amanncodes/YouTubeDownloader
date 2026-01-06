from __future__ import annotations

import logging
import shutil
from typing import Optional

from celery import shared_task
from django.db import transaction

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from jobs.models import DownloadJob, JobStatus
from jobs.cleanup import cleanup_job_files
from media.models import VideoMetadata
from media_engine.discovery import DiscoveryEngine
from media_engine.yt_dlp_wrapper import (
    YtDlpEngine,
    MetadataExtractionError,
    DownloadError,
)

logger = logging.getLogger(__name__)
channel_layer = get_channel_layer()

TMP_DIR = "tmp_downloads"


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def sanitize_error_message(
    message: Optional[str],
    max_length: int = 200,
) -> str:
    if not message:
        return "An unknown error occurred."
    return " ".join(str(message).split())[:max_length]


def get_retry_metadata(job: DownloadJob) -> dict:
    return {
        "can_retry": job.can_retry(),
        "retry_count": job.retry_count,
        "retry_after_seconds": (
            2 ** job.retry_count if job.can_retry() else None
        ),
    }


def get_current_status(job_id: str) -> Optional[str]:
    return (
        DownloadJob.objects
        .filter(id=job_id)
        .values_list("status", flat=True)
        .first()
    )


def disk_pressure_exceeded(
    path: str = TMP_DIR,
    threshold: float = 0.9,
) -> bool:
    total, used, _ = shutil.disk_usage(path)
    return (used / total) >= threshold


def emit_ws(job: DownloadJob, payload: dict) -> None:
    async_to_sync(channel_layer.group_send)(
        f"job_{job.id}",
        {
            "type": "job_progress",
            "data": payload,
        },
    )


# ─────────────────────────────────────────────────────────────
# COLLECTION TASK
# ─────────────────────────────────────────────────────────────

@shared_task(bind=True, acks_late=True)
def process_collection_job(self, parent_job_id: str):
    try:
        parent = DownloadJob.objects.get(id=parent_job_id)
    except DownloadJob.DoesNotExist:
        return

    if parent.status != JobStatus.PENDING:
        return

    parent.update_status(JobStatus.DISCOVERING)

    try:
        urls = DiscoveryEngine.extract_video_urls(parent.source_url)
    except Exception as exc:
        parent.mark_failed(
            error_code="DISCOVERY_FAILED",
            error_message=sanitize_error_message(str(exc)),
        )
        return

    if not urls:
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
        for url in urls
    ]

    parent.total_children = len(children)
    parent.update_status(JobStatus.DOWNLOADING)
    parent.save(update_fields=["total_children", "status"])

    for child in children:
        process_download_job.delay(str(child.id))


# ─────────────────────────────────────────────────────────────
# VIDEO TASK
# ─────────────────────────────────────────────────────────────

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

            # ─── DISK PRESSURE GUARD (REAL) ───
            if disk_pressure_exceeded():
                job.mark_failed(
                    error_code="DISK_FULL",
                    error_message="Server disk usage exceeded safe limit",
                )

                emit_ws(
                    job,
                    {
                        "job_id": str(job.id),
                        "status": JobStatus.FAILED,
                        "terminal": True,
                        "error_code": "DISK_FULL",
                        "error_message": "Server disk usage exceeded safe limit",
                        "can_retry": False,
                    },
                )

                cleanup_job_files(str(job.id))
                return

            job.mark_started()
            job.update_status(JobStatus.FETCHING_METADATA)

    except DownloadJob.DoesNotExist:
        return

    engine = YtDlpEngine(
        work_dir=TMP_DIR,
        proxy=None,
        cookie_file=None,
    )

    try:
        # ─── METADATA ───
        metadata = engine.extract_metadata(job.source_url)

        VideoMetadata.objects.update_or_create(
            youtube_id=metadata.get("id"),
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

        # ─── PROGRESS HOOK ───
        def on_progress(percent: float):
            status = get_current_status(job.id)

            if status == JobStatus.PAUSED:
                raise DownloadError("JOB_PAUSED")

            if status == JobStatus.CANCELLED:
                raise DownloadError("JOB_CANCELLED")

            job.update_progress(percent)

            emit_ws(
                job,
                {
                    "job_id": str(job.id),
                    "status": JobStatus.DOWNLOADING,
                    "progress": percent,
                },
            )

        # ─── DOWNLOAD ───
        engine.download(
            job.source_url,
            progress_callback=on_progress,
        )

        # ─── SUCCESS ───
        job.mark_completed()

        emit_ws(
            job,
            {
                "job_id": str(job.id),
                "status": JobStatus.COMPLETED,
                "progress": 100.0,
                "terminal": True,
            },
        )

    # ─────────────────────────────
    # PAUSE / CANCEL
    # ─────────────────────────────
    except DownloadError as exc:
        reason = str(exc)

        if reason == "JOB_PAUSED":
            job.update_status(JobStatus.PAUSED)

            emit_ws(
                job,
                {
                    "job_id": str(job.id),
                    "status": JobStatus.PAUSED,
                    "progress": job.progress_percentage,
                    "terminal": True,
                    "can_resume": True,
                },
            )
            return

        if reason == "JOB_CANCELLED":
            job.mark_cancelled()
            cleanup_job_files(str(job.id))
            return

        raise

    # ─────────────────────────────
    # METADATA FAILURE
    # ─────────────────────────────
    except MetadataExtractionError as exc:
        error_message = sanitize_error_message(str(exc))
        retry_meta = get_retry_metadata(job)

        emit_ws(
            job,
            {
                "job_id": str(job.id),
                "status": JobStatus.FAILED,
                "terminal": True,
                "error_code": "METADATA_EXTRACTION_FAILED",
                "error_message": error_message,
                **retry_meta,
            },
        )

        job.mark_failed(
            error_code="METADATA_EXTRACTION_FAILED",
            error_message=error_message,
        )

    # ─────────────────────────────
    # UNEXPECTED FAILURE
    # ─────────────────────────────
    except Exception as exc:
        logger.exception("Unexpected failure for job %s", job.id)

        error_message = sanitize_error_message(str(exc))
        retry_meta = get_retry_metadata(job)

        emit_ws(
            job,
            {
                "job_id": str(job.id),
                "status": JobStatus.FAILED,
                "terminal": True,
                "error_code": "UNEXPECTED_ERROR",
                "error_message": error_message,
                **retry_meta,
            },
        )

        job.mark_failed(
            error_code="UNEXPECTED_ERROR",
            error_message=error_message,
        )
        cleanup_job_files(str(job.id))
