from rest_framework import serializers

from jobs.models import DownloadJob


class DownloadJobCreateSerializer(serializers.ModelSerializer):
    """
    Serializer for creating a new download job.
    """

    class Meta:
        model = DownloadJob
        fields = (
            "id",
            "job_type",
            "source_url",
        )
        read_only_fields = ("id",)

    def validate_source_url(self, value: str) -> str:
        value = value.strip()

        if "youtube.com" not in value and "youtu.be" not in value:
            raise serializers.ValidationError(
                "Only YouTube URLs are supported."
            )

        return value


class DownloadJobSerializer(serializers.ModelSerializer):
    """
    Full job representation used for list & detail views.
    """

    class Meta:
        model = DownloadJob
        fields = (
            "id",
            "job_type",
            "source_url",
            "status",
            "progress_percentage",
            "retry_count",
            "created_at",
            "updated_at",
        )


class RetryJobResponseSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for retry responses.
    """

    can_retry = serializers.SerializerMethodField()

    class Meta:
        model = DownloadJob
        fields = (
            "id",
            "status",
            "retry_count",
            "can_retry",
        )

    def get_can_retry(self, obj: DownloadJob) -> bool:
        return obj.can_retry()
