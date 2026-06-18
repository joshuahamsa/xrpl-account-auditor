import pytest
from xrpl_audit.crawler import crawl, is_service_leaf


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
