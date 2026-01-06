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
    Retry a FAILED job if eligible.
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
                        {"detail": "Job is not in FAILED state."},
                        status=status.HTTP_409_CONFLICT,
                    )

                if not job.can_retry():
                    return Response(
                        {"detail": "Job is not retryable."},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                job.status = JobStatus.PENDING
                job.progress_percentage = 0.0
                job.save(update_fields=["status", "progress_percentage"])

        except DownloadJob.DoesNotExist:
            return Response(
                {"detail": "Job not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        process_download_job.delay(str(job.id))

        return Response(
            {
                "id": job.id,
                "status": job.status,
                "retry_count": job.retry_count,
                "message": "Job scheduled for retry",
            },
            status=status.HTTP_202_ACCEPTED,
        )


class PauseJobAPIView(APIView):
    """
    Pause a running job (cooperative).
    """

    def post(self, request, job_id):
        try:
            with transaction.atomic():
                job = (
                    DownloadJob.objects
                    .select_for_update()
                    .get(id=job_id)
                )

                if job.status != JobStatus.DOWNLOADING:
                    return Response(
                        {"detail": "Only DOWNLOADING jobs can be paused."},
                        status=status.HTTP_409_CONFLICT,
                    )

                job.update_status(JobStatus.PAUSED)

        except DownloadJob.DoesNotExist:
            return Response(
                {"detail": "Job not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(
            {
                "id": job.id,
                "status": job.status,
                "message": "Job paused successfully.",
            },
            status=status.HTTP_200_OK,
        )


class ResumeJobAPIView(APIView):
    """
    Resume a paused job.
    """

    def post(self, request, job_id):
        try:
            with transaction.atomic():
                job = (
                    DownloadJob.objects
                    .select_for_update()
                    .get(id=job_id)
                )

                if job.status != JobStatus.PAUSED:
                    return Response(
                        {"detail": "Only PAUSED jobs can be resumed."},
                        status=status.HTTP_409_CONFLICT,
                    )

                job.update_status(JobStatus.PENDING)

        except DownloadJob.DoesNotExist:
            return Response(
                {"detail": "Job not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        process_download_job.delay(str(job.id))

        return Response(
            {
                "id": job.id,
                "status": job.status,
                "message": "Job resumed successfully.",
            },
            status=status.HTTP_202_ACCEPTED,
        )
