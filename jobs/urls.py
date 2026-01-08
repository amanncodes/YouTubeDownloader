from django.urls import path
from jobs.views import (
    DownloadJobCreateAPIView,
    DownloadJobListAPIView,
    DownloadJobDetailAPIView,
    RetryJobAPIView,
    PauseJobAPIView,
    ResumeJobAPIView,
    CancelJobAPIView,
)

urlpatterns = [
    path("jobs/", DownloadJobCreateAPIView.as_view()),
    path("jobs/list/", DownloadJobListAPIView.as_view()),
    path("jobs/<uuid:pk>/", DownloadJobDetailAPIView.as_view()),
    path("jobs/<uuid:job_id>/retry/", RetryJobAPIView.as_view()),
    path("jobs/<uuid:job_id>/pause/", PauseJobAPIView.as_view()),
    path("jobs/<uuid:job_id>/resume/", ResumeJobAPIView.as_view()),
    path("jobs/<uuid:job_id>/cancel/", CancelJobAPIView.as_view()),
]
