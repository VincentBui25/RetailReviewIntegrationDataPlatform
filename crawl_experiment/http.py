from __future__ import annotations

from dataclasses import dataclass
import threading
import time
from typing import Callable
import urllib.error
import urllib.request


@dataclass
class FetchResult:
    url: str
    status: int | None
    body: bytes
    latency_ms: float
    retries: int
    error: str | None = None
    count_403: int = 0
    count_429: int = 0


class RateLimiter:
    def __init__(self, interval_seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.interval = max(0.0, interval_seconds)
        self.clock = clock
        self._next_at = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self.clock()
            delay = self._next_at - now
            if delay > 0:
                time.sleep(delay)
                now = self.clock()
            self._next_at = now + self.interval


class HttpClient:
    TRANSIENT = {408, 425, 429, 500, 502, 503, 504}

    def __init__(self, interval_seconds: float, timeout_seconds: float, max_retries: int) -> None:
        self.rate_limiter = RateLimiter(interval_seconds)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(0, max_retries)

    def fetch(self, url: str) -> FetchResult:
        last: FetchResult | None = None
        count_403 = 0
        count_429 = 0
        for attempt in range(self.max_retries + 1):
            self.rate_limiter.wait()
            started = time.perf_counter()
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "MealVsBasket-CrawlExperiment/0.1 (+bounded research crawl)"},
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    body = response.read()
                    status = int(response.status)
                count_403 += int(status == 403)
                count_429 += int(status == 429)
                last = FetchResult(url, status, body, (time.perf_counter() - started) * 1000, attempt, count_403=count_403, count_429=count_429)
            except urllib.error.HTTPError as exc:
                body = exc.read()
                count_403 += int(exc.code == 403)
                count_429 += int(exc.code == 429)
                last = FetchResult(url, exc.code, body, (time.perf_counter() - started) * 1000, attempt, str(exc), count_403, count_429)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last = FetchResult(url, None, b"", (time.perf_counter() - started) * 1000, attempt, str(exc), count_403, count_429)
            if last.status not in self.TRANSIENT and last.status is not None:
                return last
            if attempt < self.max_retries:
                time.sleep(min(2 ** attempt, 4))
        assert last is not None
        return last
