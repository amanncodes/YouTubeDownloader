from __future__ import annotations

import os
import json
import uuid
import logging
import shutil
from pathlib import Path
from typing import Callable, Optional, Dict, Any

import yt_dlp


logger = logging.getLogger(__name__)


class YtDlpError(Exception):
    """Base exception for yt-dlp wrapper errors."""


class MetadataExtractionError(YtDlpError):
    pass


class DownloadError(YtDlpError):
    pass


class YtDlpEngine:
    """
    Thin, deterministic wrapper around yt-dlp.

    - No Django dependency
    - No Celery dependency
    - Safe for retries
    """

    def __init__(
        self,
        work_dir: str | Path,
        proxy: Optional[str] = None,
        cookie_file: Optional[str] = None,
        user_agent: Optional[str] = None,
        sleep_interval: int = 2,
    ):
        # Working directory (job-scoped)
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        # Proxy (optional)
        self.proxy = proxy or None

        # Cookie file (optional, validated)
        self.cookie_file = None
        if cookie_file:
            try:
                cookie_path = Path(cookie_file)
                if cookie_path.exists() and cookie_path.stat().st_size > 0:
                    self.cookie_file = str(cookie_path)
            except Exception:
                # Any issue → ignore cookies safely
                self.cookie_file = None

        # User agent (optional)
        self.user_agent = user_agent

        # Throttling
        self.sleep_interval = sleep_interval

    # Public API

    def extract_metadata(self, url: str) -> Dict[str, Any]:
        """
        Extract metadata ONLY (no download).

        Cookies and proxies are treated as optional optimizations.
        """
        ydl_opts = self._base_opts(
            skip_download=True,
            write_info_json=False,
        )

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info

        except yt_dlp.utils.DownloadError as exc:
            msg = str(exc).lower()

            # Cookie-related failures
            if "cookie" in msg:
                logger.warning("Cookie error during metadata extraction")
                raise MetadataExtractionError("COOKIE_INVALID") from exc

            # Proxy / connection issues
            if any(k in msg for k in ("proxy", "connection", "timed out", "timeout")):
                logger.warning("Proxy or network error during metadata extraction")
                raise MetadataExtractionError("PROXY_FAILED") from exc

            # Real extraction failures
            logger.exception("Metadata extraction failed")
            raise MetadataExtractionError(str(exc)) from exc

        except Exception as exc:
            logger.exception("Unexpected metadata extraction error")
            raise MetadataExtractionError(str(exc)) from exc

    def download(
        self,
        url: str,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Dict[str, Any]:
        """
        Download video + audio, merge, and return info dict.

        progress_callback:
            Callable receiving progress percentage (0.0 - 100.0)
        """
        output_id = uuid.uuid4().hex
        output_template = str(self.work_dir / f"{output_id}.%(ext)s")

        ydl_opts = self._base_opts(
            skip_download=False,
            outtmpl=output_template,
            progress_callback=progress_callback,
        )

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                return info
        except Exception as exc:
            logger.exception("Download failed")
            raise DownloadError(str(exc)) from exc

    def cleanup_partial_files(self) -> None:
        """
        Remove partial / temporary files created by yt-dlp or ffmpeg.

        Safe to call multiple times.
        Never touches completed files.
        """
        if not self.work_dir.exists():
            return

        for path in self.work_dir.iterdir():
            if path.suffix in {".part", ".ytdl", ".temp"}:
                try:
                    path.unlink()
                except Exception:
                    pass

        # Optional: remove empty directory
        try:
            if not any(self.work_dir.iterdir()):
                shutil.rmtree(self.work_dir, ignore_errors=True)
        except Exception:
            pass

    # Internal helpers

    def _base_opts(
        self,
        *,
        skip_download: bool,
        outtmpl: Optional[str] = None,
        write_info_json: bool = True,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Dict[str, Any]:

        def _progress_hook(d: Dict[str, Any]):
            if not progress_callback:
                return

            if d.get("status") == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate")
                downloaded = d.get("downloaded_bytes")

                if total and downloaded:
                    percent = (downloaded / total) * 100
                    progress_callback(round(percent, 2))

            elif d.get("status") == "finished":
                progress_callback(100.0)

        opts: Dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": skip_download,
            "format": "bestvideo+bestaudio/best",
            "merge_output_format": "mp4",
            "sleep_interval": self.sleep_interval,
            "retries": 3,
            "fragment_retries": 3,
            "noplaylist": True,
            "progress_hooks": [_progress_hook],
        }

        if outtmpl:
            opts["outtmpl"] = outtmpl

        if write_info_json:
            opts["writeinfojson"] = True

        if self.proxy:
            opts["proxy"] = self.proxy

        if self.cookie_file:
            opts["cookiefile"] = self.cookie_file

        if self.user_agent:
            opts["user_agent"] = self.user_agent

        return opts
