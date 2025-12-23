# data_fetcher/utils/rate_limiter.py
import time
from collections import defaultdict
from typing import Callable

class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: int):
        self.rate, self.capacity = rate_per_sec, capacity
        self.tokens, self.last = capacity, time.time()

    def allow(self) -> bool:
        now = time.time()
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False

class RateLimiter:
    def __init__(self):
        self.buckets = defaultdict(lambda: TokenBucket(5, 10))  # default 5 rps, burst 10

    def guard(self, key: str, fn: Callable, *args, **kwargs):
        if not self.buckets[key].allow():
            from .error_handler import FetchError
            raise FetchError("RATE_LIMITED", f"rate limited for {key}", 429)
        return fn(*args, **kwargs)
