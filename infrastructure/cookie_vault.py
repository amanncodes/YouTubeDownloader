from __future__ import annotations

import random
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class CookieSession:
    """
    Represents a single cookies.txt file exported
    from a real browser session.
    """
    path: Path
    failures: int = 0
    cooldown_until: float | None = None
    last_used_at: float | None = None

    def is_available(self) -> bool:
        if self.cooldown_until is None:
            return True
        return time.time() >= self.cooldown_until


class CookieVault:
    """
    In-memory cookie rotation and health manager.

    Each worker maintains its own vault.
    DB-backed version will replace this later.
    """

    def __init__(
        self,
        cookie_files: List[str | Path],
        max_failures: int = 2,
        cooldown_seconds: int = 600,
    ):
        self._cookies: List[CookieSession] = [
            CookieSession(path=Path(p))
            for p in cookie_files
        ]
        self._max_failures = max_failures
        self._cooldown_seconds = cooldown_seconds
        self._lock = threading.Lock()


    # Selection logic
    def get_cookie(self) -> Optional[Path]:
        """
        Return a healthy cookies.txt path or None.
        """
        with self._lock:
            candidates = [
                c for c in self._cookies
                if c.is_available() and c.path.exists()
            ]

            if not candidates:
                return None

            # Prefer least recently used cookies
            candidates.sort(
                key=lambda c: c.last_used_at or 0
            )

            chosen = random.choice(candidates[:2])
            chosen.last_used_at = time.time()
            return chosen.path

    # Health reporting
    def report_success(self, cookie_path: Path):
        with self._lock:
            session = self._find(cookie_path)
            if not session:
                return

            session.failures = 0
            session.cooldown_until = None

    def report_failure(self, cookie_path: Path):
        with self._lock:
            session = self._find(cookie_path)
            if not session:
                return

            session.failures += 1

            if session.failures >= self._max_failures:
                session.cooldown_until = (
                    time.time() + self._cooldown_seconds
                )

    # Internal helpers
    def _find(self, cookie_path: Path) -> Optional[CookieSession]:
        for session in self._cookies:
            if session.path == cookie_path:
                return session
        return None
