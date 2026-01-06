from django.urls import path

from jobs.views import (
    DownloadJobCreateAPIView,
    DownloadJobListAPIView,
    DownloadJobDetailAPIView,
    RetryJobAPIView,
)

urlpatterns = [
    # Core job APIs
    path("jobs/", DownloadJobCreateAPIView.as_view()),
    path("jobs/list/", DownloadJobListAPIView.as_view()),
    path("jobs/<uuid:pk>/", DownloadJobDetailAPIView.as_view()),

    # Actions
    path("jobs/<uuid:job_id>/retry/", RetryJobAPIView.as_view()),
]
