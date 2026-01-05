from django.urls import path
from jobs.views import (
    DownloadJobCreateAPIView,
    DownloadJobListAPIView,
    DownloadJobDetailAPIView,
)

urlpatterns = [
    path("jobs/", DownloadJobCreateAPIView.as_view()),
    path("jobs/<uuid:pk>/", DownloadJobDetailAPIView.as_view()),
    path("jobs/list/", DownloadJobListAPIView.as_view()),
]
