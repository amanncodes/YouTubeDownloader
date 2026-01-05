from django.urls import re_path
from .consumers import JobProgressConsumer

websocket_urlpatterns = [
    re_path(
        r"^ws/jobs/(?P<job_id>[0-9a-f-]+)/$",
        JobProgressConsumer.as_asgi(),
    ),
]