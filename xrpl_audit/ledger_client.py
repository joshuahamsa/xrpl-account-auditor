import asyncio
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

GENESIS_LEDGER = 32570

# Substrings (lowercased) that mark a node throttling us rather than a genuine
# fetch failure. The public xrplcluster/ripple nodes close with a 1008 policy
# violation ("Connection (public) IP limit reached") when too many requests or
# connections come from one IP; that is transient and must be ridden out, not
# turned into a permanent per-account error.
_RATE_LIMIT_MARKERS = (
    "1008",
    "policy violation",
    "ip limit",
    "slow down",
    "too many",
    "rate limit",
    "try again",
)


def _is_rate_limit(exc: BaseException) -> bool:
    """True if `exc` looks like the node throttling/refusing us (vs. a real error)."""
    s = str(exc).lower()
    return any(marker in s for marker in _RATE_LIMIT_MARKERS)


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
    def __init__(self, url: str, max_retries: int = 5, backoff_base: float = 0.5,
                 rate_limit_max_retries: int = 50, rate_limit_backoff_cap: float = 60.0):
        self.url = url
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        # Rate-limits get their own, far larger retry budget and a capped
        # exponential backoff so a transient IP ban is ridden out instead of
        # cascading every queued account into `error`.
        self.rate_limit_max_retries = rate_limit_max_retries
        self.rate_limit_backoff_cap = rate_limit_backoff_cap
        self._client = None
        self._lock = asyncio.Lock()

    async def _new_connection(self):
        from xrpl.asyncio.clients import AsyncWebsocketClient
        c = AsyncWebsocketClient(self.url)
        await c.open()
        return c

    async def _get_client(self):
        """Return the shared connection, opening one under the lock if needed.

        Connection setup is serialized so concurrent workers share a single
        live connection rather than each racing to open their own.
        """
        async with self._lock:
            if self._client is None:
                self._client = await self._new_connection()
            return self._client

    async def _drop_client(self, failed) -> None:
        """Reset the shared connection, but only if `failed` is still the
        active one. Identity guard prevents one worker's transient error from
        nulling a connection another worker already reconnected and is using."""
        async with self._lock:
            if self._client is failed:
                try:
                    await self._client.close()
                except Exception:
                    pass
                self._client = None

    async def _raw_fetch(self, address: str, marker, limit: int) -> dict:
        from xrpl.models.requests import AccountTx
        last_exc = None
        err_attempts = 0   # genuine errors (small budget)
        rl_attempts = 0    # rate-limit / throttle (large budget)
        while True:
            client = await self._get_client()
            try:
                req = AccountTx(account=address, limit=limit,
                                marker=marker, forward=True)
                resp = await client.request(req)
                result = resp.result
                return {"transactions": result.get("transactions", []),
                        "marker": result.get("marker")}
            except Exception as exc:                  # reconnect + backoff
                last_exc = exc
                await self._drop_client(client)
                if _is_rate_limit(exc):
                    rl_attempts += 1
                    if rl_attempts > self.rate_limit_max_retries:
                        break
                    delay = min(self.rate_limit_backoff_cap,
                                self.backoff_base * (2 ** rl_attempts))
                    delay += random.uniform(0, delay * 0.25)   # jitter to desync workers
                else:
                    err_attempts += 1
                    if err_attempts >= self.max_retries:
                        break
                    delay = self.backoff_base * (2 ** (err_attempts - 1))
                await asyncio.sleep(delay)
        raise RuntimeError(
            f"account_tx failed after {err_attempts} errors / {rl_attempts} "
            f"rate-limits: {last_exc}")

    async def account_tx(self, address: str, marker=None, limit: int = 200):
        page = await self._raw_fetch(address, marker, limit)
        return page["transactions"], page["marker"]

    async def verify_full_history(self) -> bool:
        from xrpl.models.requests import ServerInfo
        client = await self._get_client()
        resp = await client.request(ServerInfo())
        complete = resp.result.get("info", {}).get("complete_ledgers", "")
        return _is_full_history(complete)

    async def close(self):
        if self._client is not None:
            await self._client.close()
            self._client = None
