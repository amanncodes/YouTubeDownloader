from rest_framework import status, generics
from rest_framework.response import Response

from jobs.models import DownloadJob
from jobs.serializers import (
    DownloadJobCreateSerializer,
    DownloadJobSerializer,
)

from jobs.tasks import process_download_job, process_collection_job


class DownloadJobCreateAPIView(generics.CreateAPIView):
    queryset = DownloadJob.objects.all()
    serializer_class = DownloadJobCreateSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job = serializer.save(status="PENDING")

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
