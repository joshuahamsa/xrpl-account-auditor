import pytest
from xrpl_audit.ledger_client import paginate_all

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
