import time
from typing import Any, Dict, Optional
from config.settings import settings

class TTLCache:
    """RAM-only, TTL-bound cache for reference data (aliases, MCCs, rules)."""
    def __init__(self, ttl: Optional[int] = None):
        self._ttl = ttl or settings.TTL_SECONDS
        self._store: Dict[str, Any] = {}
        self._ts: Dict[str, float] = {}

    def get(self, key: str) -> Optional[Any]:
        val = self._store.get(key)
        ts = self._ts.get(key)
        if val is None or ts is None:
            return None
        if time.time() - ts > self._ttl:
            self.delete(key)
            return None
        return val

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value
        self._ts[key] = time.time()

    def delete(self, key: str) -> None:
        self._store.pop(key, None)
        self._ts.pop(key, None)

    def clear(self) -> None:
        self._store.clear()
        self._ts.clear()

# Preload typical reference maps (can be refreshed on a schedule)
cache = TTLCache()
cache.set("merchant_aliases", {"AMZN Mkt": "Amazon", "GOOGLE*SVCS": "Google"})
cache.set("mcc_map", {"5814": "Fast Food", "4111": "Transport"})
cache.set("category_rules", {"Amazon": "Shopping", "Google": "Services"})
