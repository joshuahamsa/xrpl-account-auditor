import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

GENESIS_LEDGER = 32570


def _is_full_history(complete_ledgers: str) -> bool:
    """True if the node's complete_ledgers range reaches back to genesis (<=32570)."""
    if not complete_ledgers:
        return False
    low = complete_ledgers.split(",")[0].split("-")[0].strip()
    try:
        return int(low) <= GENESIS_LEDGER
    except ValueError:
        return False


class LedgerSource(Protocol):
    async def account_tx(self, address: str, marker=None, limit: int = 200) -> tuple[list[dict], dict | None]:
        ...

async def paginate_all(
    fetch: Callable[[str, object, int], Awaitable[dict]],
    address: str,
    limit: int = 200,
) -> AsyncIterator[dict]:
    """Drive a raw account_tx fetch function across all marker pages."""
    marker = None
    while True:
        page = await fetch(address, marker, limit)
        for tx in page.get("transactions", []):
            yield tx
        marker = page.get("marker")
        if not marker:
            return

class LedgerClient:
    def __init__(self, url: str, max_retries: int = 5, backoff_base: float = 0.5):
        self.url = url
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self._client = None

    async def _raw_fetch(self, address: str, marker, limit: int) -> dict:
        from xrpl.models.requests import AccountTx
        from xrpl.asyncio.clients import AsyncWebsocketClient
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                if self._client is None:
                    self._client = AsyncWebsocketClient(self.url)
                    await self._client.open()
                req = AccountTx(account=address, limit=limit,
                                marker=marker, forward=True)
                resp = await self._client.request(req)
                result = resp.result
                return {"transactions": result.get("transactions", []),
                        "marker": result.get("marker")}
            except Exception as exc:                  # reconnect + backoff
                last_exc = exc
                self._client = None
                await asyncio.sleep(self.backoff_base * (2 ** attempt))
        raise RuntimeError(f"account_tx failed after {self.max_retries} retries: {last_exc}")

    async def account_tx(self, address: str, marker=None, limit: int = 200):
        page = await self._raw_fetch(address, marker, limit)
        return page["transactions"], page["marker"]

    async def verify_full_history(self) -> bool:
        from xrpl.models.requests import ServerInfo
        if self._client is None:
            from xrpl.asyncio.clients import AsyncWebsocketClient
            self._client = AsyncWebsocketClient(self.url)
            await self._client.open()
        resp = await self._client.request(ServerInfo())
        complete = resp.result.get("info", {}).get("complete_ledgers", "")
        return _is_full_history(complete)

    async def close(self):
        if self._client is not None:
            await self._client.close()
            self._client = None
