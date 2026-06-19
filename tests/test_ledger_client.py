import pytest
from xrpl_audit.ledger_client import paginate_all, LedgerClient, _is_rate_limit


class _Conn:
    def __init__(self): self.closed = False
    async def close(self): self.closed = True


@pytest.mark.asyncio
async def test_drop_client_only_resets_matching_connection():
    """A worker hitting a transient error must not null another worker's
    freshly-reconnected client. Reset is identity-guarded."""
    client = LedgerClient("wss://example")
    a, b = _Conn(), _Conn()
    client._client = a
    # b failed but is no longer the active client -> must be a no-op
    await client._drop_client(b)
    assert client._client is a
    assert a.closed is False
    # a is the active client -> drop and close it
    await client._drop_client(a)
    assert client._client is None
    assert a.closed is True

class _FakeRaw:
    """Simulates raw account_tx responses with markers."""
    def __init__(self, pages): self.pages = pages; self.calls = 0
    async def fetch(self, address, marker, limit):
        page = self.pages[self.calls]; self.calls += 1
        return page  # {"transactions": [...], "marker": ... or None}

def test_is_rate_limit_detects_node_connection_limit():
    """The public-node 1008 close ('IP limit reached') must be classified as a
    rate-limit so the client backs off instead of giving up."""
    assert _is_rate_limit(RuntimeError(
        "received 1008 (policy violation) Connection (public) IP limit reached"))
    assert _is_rate_limit(Exception("Connection (public) IP limit reached"))
    assert _is_rate_limit(Exception("please slow down"))
    # A genuine non-throttle error is NOT a rate-limit.
    assert not _is_rate_limit(RuntimeError("malformed account_tx response"))


class _RateLimitThenOk:
    """A fake ws connection that raises a 1008 rate-limit `fail_times` times,
    then returns a valid account_tx page."""
    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.calls = 0

    async def request(self, req):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError(
                "received 1008 (policy violation) Connection (public) IP limit reached")

        class _Resp:
            result = {"transactions": [{"hash": "A"}], "marker": None}
        return _Resp()

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_raw_fetch_survives_persistent_rate_limit(monkeypatch):
    """A rate-limit that outlasts the normal retry budget must NOT raise.

    The old code gave up after max_retries (~5) and the crawler turned the
    account into an `error`, cascading the whole queue. Rate-limits get their
    own, much larger retry budget so a transient IP ban is ridden out.
    """
    client = LedgerClient("wss://example", max_retries=2,
                          rate_limit_max_retries=10, backoff_base=0.0)
    conn = _RateLimitThenOk(fail_times=5)   # 5 > max_retries(2): only RL budget survives this
    monkeypatch.setattr(client, "_get_client", lambda: _coro(conn))
    monkeypatch.setattr(client, "_drop_client", lambda failed: _coro(None))
    sleeps = []
    async def fake_sleep(d): sleeps.append(d)
    monkeypatch.setattr("xrpl_audit.ledger_client.asyncio.sleep", fake_sleep)

    page = await client._raw_fetch("rX", None, 200)
    assert page["transactions"] == [{"hash": "A"}]
    assert conn.calls == 6                  # 5 rate-limited + 1 success
    assert len(sleeps) == 5                  # backed off after each rate-limit


async def _coro(value):
    return value


class _HangThenOk:
    """A fake ws connection whose request() hangs (never responds) the first
    `hang_times` calls, then returns a valid page. Models a half-open
    connection where the node goes silent without sending a close frame."""
    def __init__(self, hang_times):
        self.hang_times = hang_times
        self.calls = 0

    async def request(self, req):
        self.calls += 1
        if self.calls <= self.hang_times:
            await asyncio.sleep(3600)   # hang until wait_for cancels us

        class _Resp:
            result = {"transactions": [{"hash": "A"}], "marker": None}
        return _Resp()

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_raw_fetch_times_out_on_silent_hang_and_recovers(monkeypatch):
    """A request that never returns must not hang forever. A per-request
    timeout turns the silent stall into a retryable error so reconnect+backoff
    can recover, instead of blocking the worker indefinitely."""
    client = LedgerClient("wss://example", max_retries=5, backoff_base=0.0,
                          request_timeout=0.02)
    conn = _HangThenOk(hang_times=2)
    monkeypatch.setattr(client, "_get_client", lambda: _coro(conn))
    monkeypatch.setattr(client, "_drop_client", lambda failed: _coro(None))

    page = await client._raw_fetch("rX", None, 200)
    assert page["transactions"] == [{"hash": "A"}]
    assert conn.calls == 3   # 2 timed-out hangs + 1 success


class _RaisingConn:
    """A fake connection whose request() always fails (e.g. socket closed)."""
    async def request(self, req):
        raise RuntimeError("Websocket is not open")

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_verify_full_history_degrades_instead_of_crashing(monkeypatch):
    """A flaky startup connection must not abort the crawl. verify_full_history
    returns None ('unknown') and drops the broken client so the next request
    reconnects, rather than propagating the exception."""
    client = LedgerClient("wss://example", request_timeout=0.02)
    conn = _RaisingConn()
    monkeypatch.setattr(client, "_get_client", lambda: _coro(conn))
    dropped = []
    monkeypatch.setattr(client, "_drop_client",
                        lambda failed: _coro(dropped.append(failed)))

    result = await client.verify_full_history()
    assert result is None
    assert dropped == [conn]   # broken connection dropped -> next call reconnects


@pytest.mark.asyncio
async def test_paginate_all_follows_markers():
    raw = _FakeRaw([
        {"transactions": [{"tx": {"hash": "A"}}], "marker": {"m": 1}},
        {"transactions": [{"tx": {"hash": "B"}}], "marker": None},
    ])
    txs = [t async for t in paginate_all(raw.fetch, "rX", limit=1)]
    assert [t["tx"]["hash"] for t in txs] == ["A", "B"]
    assert raw.calls == 2
