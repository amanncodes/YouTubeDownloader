from rest_framework import status, generics
from rest_framework.response import Response
from rest_framework.views import APIView

from django.db import transaction

from jobs.models import DownloadJob, JobStatus
from jobs.serializers import (
    DownloadJobCreateSerializer,
    DownloadJobSerializer,
    RetryJobResponseSerializer,
)

from jobs.tasks import process_download_job, process_collection_job


class DownloadJobCreateAPIView(generics.CreateAPIView):
    queryset = DownloadJob.objects.all()
    serializer_class = DownloadJobCreateSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job = serializer.save(status=JobStatus.PENDING)

        # Enqueue async task
        if job.job_type in ("PLAYLIST", "CHANNEL"):
            process_collection_job.delay(str(job.id))
        else:
            process_download_job.delay(str(job.id))

        return Response(
            DownloadJobSerializer(job).data,
            status=status.HTTP_201_CREATED,
        )


class DownloadJobListAPIView(generics.ListAPIView):
    queryset = DownloadJob.objects.all().order_by("-created_at")
    serializer_class = DownloadJobSerializer


class DownloadJobDetailAPIView(generics.RetrieveAPIView):
    queryset = DownloadJob.objects.all()
    serializer_class = DownloadJobSerializer


class RetryJobAPIView(APIView):
    """
    Retry a FAILED job if retry eligibility allows it.
    """

    def post(self, request, job_id):
        try:
            with transaction.atomic():
                job = (
                    DownloadJob.objects
                    .select_for_update()
                    .get(id=job_id)
                )

                if job.status != JobStatus.FAILED:
                    return Response(
                        {"detail": "Job is not in FAILED state"},
                        status=status.HTTP_409_CONFLICT,
                    )

                if not job.can_retry():
                    return Response(
                        {"detail": "Job is not retryable"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                # Reset state for retry
                job.status = JobStatus.PENDING
                job.progress_percentage = 0.0
                job.save(
                    update_fields=["status", "progress_percentage"]
                )

        except DownloadJob.DoesNotExist:
            return Response(
                {"detail": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Re-enqueue task (outside transaction)
        if job.job_type in ("PLAYLIST", "CHANNEL"):
            process_collection_job.delay(str(job.id))
        else:
            process_download_job.delay(str(job.id))

        serializer = RetryJobResponseSerializer(job)

        return Response(
            {
                **serializer.data,
                "message": "Job scheduled for retry",
            },
            status=status.HTTP_202_ACCEPTED,
        )

class CancelJobAPIView(APIView):
    """
    Cancel a running or pending job.
    """

    def post(self, request, job_id):
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
                    return Response(
                        {"detail": "Job is already in a terminal state"},
                        status=status.HTTP_409_CONFLICT,
                    )

                job.status = JobStatus.CANCELLED
                job.save(update_fileds=["status"])

        except DownloadJob.DoesnNotExist:
            return Response(
                {"detail": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(
            {
                "id": str(job.id),
                "status": job.status,
                "message": "Job cancellation requested",
            },
            status=status.HTTP_202_ACCEPTED,
        )