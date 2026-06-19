import asyncio
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

GENESIS_LEDGER = 32570

# Substrings (lowercased) marking a transient transport/throttle condition that
# must be ridden out patiently rather than turned into a permanent per-account
# error. The public xrplcluster/ripple nodes close with a 1008 policy violation
# ("Connection (public) IP limit reached") under load; that 1008 is often
# swallowed by the client's background handler, so the *next* request instead
# fails with a closed-socket symptom ("Websocket is not open", 1006). All of
# these are connection-level and should back off, not give up.
_RATE_LIMIT_MARKERS = (
    "1008",
    "policy violation",
    "ip limit",
    "slow down",
    "too many",
    "rate limit",
    "try again",
    "websocket is not open",
    "connection closed",
    "connection is closed",
    "1006",
    "1001",
)


def _is_rate_limit(exc: BaseException) -> bool:
    """True if `exc` is a transient throttle/transport failure worth a patient
    retry (vs. a genuine, give-up-fast error)."""
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
                 rate_limit_max_retries: int = 50, rate_limit_backoff_cap: float = 60.0,
                 request_timeout: float = 30.0, min_request_interval: float = 0.0):
        self.url = url
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        # Global request spacing: stay under the public node's per-IP cap so we
        # never trip a ban in the first place (backoff only recovers *after* a
        # ban). Shared across all workers via _rate_lock.
        self.min_request_interval = min_request_interval
        self._rate_lock = asyncio.Lock()
        self._next_allowed = 0.0
        # A half-open connection (node goes silent without a close frame) makes
        # client.request() block forever. Bound every request so a silent stall
        # becomes a retryable timeout instead of hanging the worker.
        self.request_timeout = request_timeout
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

    async def _throttle(self) -> None:
        """Block until the next request is allowed, enforcing a global minimum
        interval between requests across all workers."""
        if self.min_request_interval <= 0:
            return
        async with self._rate_lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            wait = self._next_allowed - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = loop.time()
            self._next_allowed = max(now, self._next_allowed) + self.min_request_interval

    async def _raw_fetch(self, address: str, marker, limit: int) -> dict:
        from xrpl.models.requests import AccountTx
        last_exc = None
        err_attempts = 0   # genuine errors (small budget)
        rl_attempts = 0    # rate-limit / throttle (large budget)
        while True:
            await self._throttle()
            client = await self._get_client()
            try:
                req = AccountTx(account=address, limit=limit,
                                marker=marker, forward=True)
                resp = await asyncio.wait_for(client.request(req),
                                              self.request_timeout)
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

    async def verify_full_history(self) -> bool | None:
        """True/False if the node advertises full history; None if the check
        could not run (connection failed/timed out). A failed preflight must
        not abort the crawl, so the broken client is dropped and None returned."""
        from xrpl.models.requests import ServerInfo
        await self._throttle()
        client = await self._get_client()
        try:
            resp = await asyncio.wait_for(client.request(ServerInfo()),
                                          self.request_timeout)
        except Exception:
            await self._drop_client(client)
            return None
        complete = resp.result.get("info", {}).get("complete_ledgers", "")
        return _is_full_history(complete)

    async def close(self):
        if self._client is not None:
            await self._client.close()
            self._client = None
