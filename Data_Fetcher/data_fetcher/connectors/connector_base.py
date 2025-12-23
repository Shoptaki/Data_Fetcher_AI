# data_fetcher/connectors/connector_base.py
'''
defines a base connector class for sending HTTP POST requests with retries, timeouts, and error handling.
'''

from dataclasses import dataclass
import httpx
from typing import Any, Dict
from ..utils.error_handler import FetchError

@dataclass
class RequestCtx:
    access_token: str
    timeout: int
    retries: int
    extra: Dict[str, Any]

class ConnectorBase:
    def __init__(self, base_url: str):
        self.base_url = base_url

    async def _post_json(self, path: str, json: Dict, ctx: RequestCtx) -> Dict:
        for attempt in range(ctx.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=ctx.timeout) as client:
                    r = await client.post(f"{self.base_url}{path}", json=json)
                    r.raise_for_status()
                    return r.json()
            except Exception as e:
                if attempt == ctx.retries:
                    raise FetchError("BANK_FETCH_FAILED", str(e), 502, {"path": path})








