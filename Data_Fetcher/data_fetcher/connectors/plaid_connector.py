# data_fetcher/connectors/plaid_connector.py
from .connector_base import ConnectorBase, RequestCtx

class PlaidConnector(ConnectorBase):
    async def fetch_balance(self, ctx: RequestCtx) -> dict:
        payload = {"access_token": ctx.access_token}
        return await self._post_json("/accounts/balance/get", payload, ctx)

    async def fetch_auth(self, ctx: RequestCtx) -> dict:
        payload = {"access_token": ctx.access_token}
        return await self._post_json("/auth/get", payload, ctx)
