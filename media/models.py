from __future__ import annotations

from django.db import models
from django.utils import timezone
from django.contrib.postgres.indexes import GinIndex

from jobs.models import DownloadJob


class VideoMetadata(models.Model):
    """
    Canonical metadata record for a YouTube video.

    Stores the full yt-dlp JSON plus extracted,
    queryable fields for indexing and UI use.
    """

    youtube_id = models.CharField(
        max_length=32,
        unique=True,
        db_index=True,
        help_text="YouTube video ID"
    )

    # Relationship to the job that fetched it
    job = models.ForeignKey(
        DownloadJob,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="video_metadata",
    )

    # Extracted / indexed fields
    title = models.TextField()
    channel_id = models.CharField(max_length=64, db_index=True)
    channel_name = models.TextField()
    duration_seconds = models.PositiveIntegerField(null=True, blank=True)
    upload_date = models.DateField(null=True, blank=True)

    view_count = models.BigIntegerField(null=True, blank=True)
    like_count = models.BigIntegerField(null=True, blank=True)

    # Full raw yt-dlp output
    raw_metadata = models.JSONField()

    extracted_at = models.DateTimeField(default=timezone.now)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-extracted_at"]
        indexes = [
            models.Index(fields=["youtube_id"]),
            models.Index(fields=["channel_id"]),
            GinIndex(fields=["raw_metadata"]),
        ]

    def __str__(self) -> str:
        return f"{self.youtube_id} | {self.title}"
