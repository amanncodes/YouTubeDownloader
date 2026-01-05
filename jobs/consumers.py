import json
from channels.generic.websocket import AsyncWebsocketConsumer


class JobProgressConsumer(AsyncWebsocketConsumer):
    """
    Streams real-time progress updates for a single job.
    One WebSocket connection == one job.
    """

    async def connect(self):
        # job_id comes from the URL
        self.job_id = self.scope["url_route"]["kwargs"]["job_id"]
        self.group_name = f"job_{self.job_id}"

        # Join job-specific group
        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name,
        )

        await self.accept()

    async def disconnect(self, close_code):
        # Leave job group
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name,
        )

    async def job_progress(self, event):
        """
        Receive progress event from Celery via channel layer
        """
        await self.send(
            text_data=json.dumps(event["data"])
        )
