from __future__ import annotations

import random
import time
import threading
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Proxy:
    url: str
    proxy_type: str  # "residential" | "datacenter"
    failures: int = 0
    last_failed_at: float | None = None
    cooldown_until: float | None = None

    def is_available(self) -> bool:
        if self.cooldown_until is None:
            return True
        return time.time() >= self.cooldown_until


class ProxyPool:
    """
    In-memory proxy pool with health scoring and cooldowns.

    This is worker-local and deterministic.
    DB-backed version will replace this later.
    """

    def __init__(
        self,
        proxies: List[Proxy],
        max_failures: int = 3,
        cooldown_seconds: int = 300,
    ):
        self._proxies = proxies
        self._max_failures = max_failures
        self._cooldown_seconds = cooldown_seconds
        self._lock = threading.Lock()


    # Selection logic
    def get_proxy(self, proxy_type: Optional[str] = None) -> Optional[str]:
        """
        Return a healthy proxy URL or None.
        """
        with self._lock:
            candidates = [
                p for p in self._proxies
                if p.is_available()
                and (proxy_type is None or p.proxy_type == proxy_type)
            ]

            if not candidates:
                return None

            # Prefer least-failed proxies
            candidates.sort(key=lambda p: p.failures)

            chosen = random.choice(candidates[:3])
            return chosen.url

    # Health reporting
    def report_success(self, proxy_url: str):
        with self._lock:
            proxy = self._find(proxy_url)
            if not proxy:
                return

            proxy.failures = 0
            proxy.cooldown_until = None
            proxy.last_failed_at = None

    def report_failure(self, proxy_url: str):
        with self._lock:
            proxy = self._find(proxy_url)
            if not proxy:
                return

            proxy.failures += 1
            proxy.last_failed_at = time.time()

            if proxy.failures >= self._max_failures:
                proxy.cooldown_until = (
                    time.time() + self._cooldown_seconds
                )

    # Internal helpers
    def _find(self, proxy_url: str) -> Optional[Proxy]:
        for proxy in self._proxies:
            if proxy.url == proxy_url:
                return proxy
        return None
