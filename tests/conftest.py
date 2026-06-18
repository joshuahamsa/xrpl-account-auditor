import pytest
from xrpl_audit.storage import Store

@pytest.fixture
def store():
    s = Store(":memory:")
    s.init_schema()
    return s

from xrpl_audit.ledger_client import LedgerSource

class FakeLedger:
    """Maps address -> list of account_tx entries. Single-page (no markers)."""
    def __init__(self, by_address: dict[str, list[dict]]):
        self.by_address = by_address
        self.requested: list[str] = []
    async def account_tx(self, address, marker=None, limit=200):
        self.requested.append(address)
        return self.by_address.get(address, []), None

@pytest.fixture
def fake_ledger_factory():
    return FakeLedger
