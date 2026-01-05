from __future__ import annotations
from typing import List
import yt_dlp


class DiscoveryEngine:
    """
    Discovers video URLs from playlists or channels
    WITHOUT downloading media.
    """

    @staticmethod
    def extract_video_urls(url: str) -> List[str]:
        ydl_opts = {
            "quiet": True,
            "extract_flat": True,
            "skip_download": True,
            "forcejson": True,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        entries = info.get("entries") or []

        urls = []
        for entry in entries:
            video_id = entry.get("id")
            if video_id:
                urls.append(f"https://www.youtube.com/watch?v={video_id}")

        return urls
