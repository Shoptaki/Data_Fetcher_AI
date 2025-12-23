# data_fetcher/utils/error_handler.py
from typing import Dict

class FetchError(Exception):
    def __init__(self, code: str, message: str, status: int = 502, meta: Dict | None = None):
        super().__init__(message)
        self.code, self.status, self.meta = code, status, meta or {}

def map_exception(e: Exception) -> FetchError:
    if isinstance(e, FetchError):
        return e
    # add mappings for httpx.TimeoutException, etc.
    return FetchError("FETCH_UNEXPECTED", str(e), 502)


