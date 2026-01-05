from rest_framework import serializers
from jobs.models import DownloadJob


class DownloadJobCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = DownloadJob
        fields = (
            "id",
            "job_type",
            "source_url",
        )
        read_only_fields = ("id",)

    def validate_source_url(self, value: str) -> str:
        if "youtube.com" not in value and "youtu.be" not in value:
            raise serializers.ValidationError(
                "Only YouTube URLs are supported."
            )
        return value


class DownloadJobSerializer(serializers.ModelSerializer):
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
