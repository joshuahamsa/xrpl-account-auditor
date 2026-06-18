import asyncio
import json
import pytest
from xrpl_audit.crawler import crawl, is_service_leaf
from xrpl_audit.storage import Store


class _MultiPageLedger:
    def __init__(self, pages_by_addr):  # addr -> list of (txs, marker) tuples
        self.pages_by_addr = pages_by_addr
        self.calls = {}
    async def account_tx(self, address, marker=None, limit=200):
        i = self.calls.get(address, 0)
        self.calls[address] = i + 1
        pages = self.pages_by_addr.get(address, [([], None)])
        return pages[min(i, len(pages)-1)]


def test_marker_persistence(tmp_path):
    """Markers are cleared after full pagination; tx_count is cumulative."""
    store = Store(str(tmp_path / "db.sqlite"))
    store.init_schema()
    ledger = _MultiPageLedger({
        "rSeed": [
            ([{"hash": "tx1"}], {"m": 1}),
            ([{"hash": "tx2"}], None),
        ]
    })
    asyncio.run(crawl("rSeed", store, ledger, workers=1, max_hops=0))
    acct = store.get_account("rSeed")
    assert acct["last_marker"] is None
    assert acct["tx_count"] == 2


def test_resume_from_pending(tmp_path):
    """resume=True seeds queue from pending_accounts, skipping 'done' accounts."""
    store = Store(str(tmp_path / "db.sqlite"))
    store.init_schema()
    # rSeed is already done
    store.upsert_account("rSeed", hop_depth=0, crawl_status="done", tx_count=5)
    # rPend is pending with a saved marker
    store.upsert_account("rPend", hop_depth=1, crawl_status="pending", tx_count=3)
    store.set_marker("rPend", json.dumps({"m": 1}))

    ledger = _MultiPageLedger({
        "rPend": [([{"hash": "txTail"}], None)],
    })
    asyncio.run(crawl("rSeed", store, ledger, workers=1, max_hops=0, resume=True))
    acct = store.get_account("rPend")
    assert acct["crawl_status"] == "done"
    assert acct["last_marker"] is None


def _payment(h, src, dst, amount="10", created=False):
    meta = {"TransactionResult": "tesSUCCESS", "AffectedNodes": []}
    if created:
        meta["AffectedNodes"] = [{"CreatedNode": {"LedgerEntryType": "AccountRoot",
                                                   "NewFields": {"Account": dst}}}]
    return {"tx": {"TransactionType": "Payment", "Account": src, "Destination": dst,
                   "Amount": amount, "Fee": "12", "hash": h},
            "meta": meta, "ledger_index": 1, "tx_close_time": 700000000}


def test_is_service_leaf():
    assert is_service_leaf(501, 500) is True
    assert is_service_leaf(500, 500) is False


@pytest.mark.asyncio
async def test_crawl_expands_activation_chain(store, fake_ledger_factory):
    # rSeed activated rChild; rChild paid rGrand
    ledger = fake_ledger_factory({
        "rSeed":  [_payment("H1", "rSeed", "rChild", "20000000", created=True)],
        "rChild": [_payment("H2", "rChild", "rGrand", "5")],
        "rGrand": [],
    })
    await crawl("rSeed", store, ledger, workers=2, max_hops=4, degree_cap=500, max_accounts=100)
    addrs = {a["address"] for a in store.iter_accounts()}
    assert {"rSeed", "rChild", "rGrand"} <= addrs
    assert store.get_account("rChild")["crawl_status"] == "done"
    assert store.get_account("rSeed")["hop_depth"] == 0
    assert store.get_account("rGrand")["hop_depth"] == 2


@pytest.mark.asyncio
async def test_crawl_respects_max_hops(store, fake_ledger_factory):
    ledger = fake_ledger_factory({
        "rSeed":  [_payment("H1", "rSeed", "rA", created=True)],
        "rA":     [_payment("H2", "rA", "rB", created=True)],
        "rB":     [_payment("H3", "rB", "rC", created=True)],
        "rC": [], "rB_unused": [],
    })
    await crawl("rSeed", store, ledger, workers=2, max_hops=1, degree_cap=500, max_accounts=100)
    # rB is at hop 2 > max_hops 1: recorded as edge target but never fetched
    assert "rB" not in ledger.requested


@pytest.mark.asyncio
async def test_crawl_treats_high_degree_as_leaf(store, fake_ledger_factory):
    # rSeed pays 3 distinct accounts; degree_cap=2 -> rSeed is a leaf, children NOT expanded
    ledger = fake_ledger_factory({
        "rSeed": [_payment("H1", "rSeed", "rA"), _payment("H2", "rSeed", "rB"),
                  _payment("H3", "rSeed", "rC")],
        "rA": [], "rB": [], "rC": [],
    })
    await crawl("rSeed", store, ledger, workers=2, max_hops=4, degree_cap=2, max_accounts=100)
    assert store.get_account("rSeed")["is_service_leaf"] == 1
    assert store.get_account("rSeed")["crawl_status"] == "leaf"
    assert ledger.requested == ["rSeed"]   # children never fetched
