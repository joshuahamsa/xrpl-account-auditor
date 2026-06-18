# tests/test_storage.py
from xrpl_audit.models import ParsedTx, Edge

def _tx(h="H1", sender="rA", dst="rB"):
    return ParsedTx(tx_hash=h, ledger_index=5, close_time=100, tx_type="Payment",
                    sender=sender, destination=dst, amount="10", currency="XRP",
                    issuer=None, fee="12", result="tesSUCCESS", edges=[])

def test_upsert_and_get_account(store):
    store.upsert_account("rA", hop_depth=0, crawl_status="pending")
    store.upsert_account("rA", tx_count=3)            # partial update keeps hop_depth
    a = store.get_account("rA")
    assert a["hop_depth"] == 0 and a["tx_count"] == 3 and a["crawl_status"] == "pending"

def test_insert_transaction_idempotent(store):
    store.insert_transaction(_tx())
    store.insert_transaction(_tx())                   # same hash, ignored
    assert store.counts()["transactions"] == 1

def test_insert_edge_idempotent(store):
    e = Edge(src="rA", dst="rB", edge_type="payment")
    store.insert_edge(e, tx_hash="H1", ledger_index=5)
    store.insert_edge(e, tx_hash="H1", ledger_index=5)
    assert store.counts()["edges"] == 1

def test_counterparty_count(store):
    store.upsert_account("rA", crawl_status="pending")
    store.record_counterparty("rA", "rB")
    store.record_counterparty("rA", "rC")
    store.record_counterparty("rA", "rB")             # duplicate, no double count
    assert store.get_account("rA")["counterparty_count"] == 2

def test_pending_accounts(store):
    store.upsert_account("rA", crawl_status="pending")
    store.upsert_account("rB", crawl_status="done")
    assert store.pending_accounts() == ["rA"]
