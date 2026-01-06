import uuid
from django.db import models
from django.utils import timezone


class JobStatus(models.TextChoices):
    """
    Canonical job state machine.
    A job MUST always be in exactly one of these states.
    """
    PENDING = "PENDING", "Pending"
    QUEUED = "QUEUED", "Queued"
    FETCHING_METADATA = "FETCHING_METADATA", "Fetching Metadata"
    DOWNLOADING = "DOWNLOADING", "Downloading"
    MUXING = "MUXING", "Muxing"
    UPLOADING = "UPLOADING", "Uploading"
    COMPLETED = "COMPLETED", "Completed"
    FAILED = "FAILED", "Failed"
    CANCELLED = "CANCELLED", "Cancelled"


class JobType(models.TextChoices):
    """
    Logical classification of the job.
    Determines orchestration strategy.
    """
    VIDEO = "VIDEO", "Single Video"
    PLAYLIST = "PLAYLIST", "Playlist"
    CHANNEL = "CHANNEL", "Channel"


class DownloadJob(models.Model):
    """
    Persistent representation of work to be executed.

    This model is the source of truth.
    Celery tasks are ephemeral; jobs are durable.
    """

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )

    # Self-referential parent for DAG support
    parent = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        related_name="children",
        on_delete=models.CASCADE
    )

    job_type = models.CharField(
        max_length=20,
        choices=JobType.choices
    )

    source_url = models.URLField(
        help_text="YouTube video / playlist / channel URL"
    )

    status = models.CharField(
        max_length=32,
        choices=JobStatus.choices,
        default=JobStatus.PENDING,
        db_index=True
    )

    progress_percentage = models.FloatField(
        default=0.0,
        help_text="0.0 to 100.0"
    )

    retry_count = models.PositiveIntegerField(
        default=0
    )

    max_retries = models.PositiveIntegerField(
        default=3
    )

    error_code = models.TextField(
        null=True,
        blank=True,
        help_text="Short machine-readable failure reason"
    )

    error_message = models.TextField(
        null=True,
        blank=True,
        help_text="Human-readable failure details"
    )

    created_at = models.DateTimeField(
        auto_now_add=True
    )

    updated_at = models.DateTimeField(
        auto_now=True
    )

    started_at = models.DateTimeField(
        null=True,
        blank=True
    )

    completed_at = models.DateTimeField(
        null=True,
        blank=True
    )

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["job_type"]),
        ]

    # State transition helpers

    def mark_started(self):
        if not self.started_at:
            self.started_at = timezone.now()
        self.save(update_fields=["started_at", "updated_at"])

    def update_status(self, new_status: JobStatus):
        self.status = new_status
        self.save(update_fields=["status", "updated_at"])

    def update_progress(self, value: float):
        self.progress_percentage = max(0.0, min(100.0, value))
        self.save(update_fields=["progress_percentage", "updated_at"])

    def mark_failed(self, error_code: str, error_message: str | None = None):
        self.status = JobStatus.FAILED
        self.error_code = error_code
        self.error_message = error_message
        self.save(update_fields=[
            "status",
            "error_code",
            "error_message",
            "updated_at"
        ])

    def mark_completed(self):
        self.status = JobStatus.COMPLETED
        self.progress_percentage = 100.0
        self.completed_at = timezone.now()
        self.save(update_fields=[
            "status",
            "progress_percentage",
            "completed_at",
            "updated_at"
        ])


    # Retry & DAG logic

    def can_retry(self) -> bool:
        return (
            self.status == JobStatus.FAILED
            and self.retry_count < self.MAX_RETRIES
        )

    def increment_retry(self):
        self.retry_count += 1
        self.save(update_fields=["retry_count", "updated_at"])

    def is_parent(self) -> bool:
        return self.parent is None

    def is_child(self) -> bool:
        return self.parent is not None

    def recompute_parent_progress(self):
        """
        Aggregate child progress into parent.
        Safe to call repeatedly.
        """
        if not self.is_parent():
            return

        children = self.children.all()
        if not children.exists():
            return

        avg_progress = sum(
            child.progress_percentage for child in children
        ) / children.count()

        self.progress_percentage = avg_progress

        # Auto-complete parent if all children completed
        if all(child.status == JobStatus.COMPLETED for child in children):
            self.status = JobStatus.COMPLETED
            self.completed_at = timezone.now()

        self.save(update_fields=[
            "progress_percentage",
            "status",
            "completed_at",
            "updated_at"
        ])

    def __str__(self):
        return f"{self.job_type} | {self.status} | {self.source_url}"
