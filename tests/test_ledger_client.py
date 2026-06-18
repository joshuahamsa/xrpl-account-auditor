import pytest
from xrpl_audit.ledger_client import paginate_all, LedgerClient


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

@pytest.mark.asyncio
async def test_paginate_all_follows_markers():
    raw = _FakeRaw([
        {"transactions": [{"tx": {"hash": "A"}}], "marker": {"m": 1}},
        {"transactions": [{"tx": {"hash": "B"}}], "marker": None},
    ])
    txs = [t async for t in paginate_all(raw.fetch, "rX", limit=1)]
    assert [t["tx"]["hash"] for t in txs] == ["A", "B"]
    assert raw.calls == 2
