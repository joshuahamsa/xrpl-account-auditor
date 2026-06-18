from xrpl_audit.models import Edge, ParsedTx
from xrpl_audit.signals import (
    compute_key_signer_signals,
    compute_funding_signals,
    compute_counterparty_nft_signals,
    compute_behavioral_signals,
)

def _seed_edges(store, edges):
    for i, (s, d, t) in enumerate(edges):
        store.upsert_account(s); store.upsert_account(d)
        store.insert_edge(Edge(s, d, t), tx_hash=f"H{i}", ledger_index=i)

def test_regular_key_signal(store):
    _seed_edges(store, [("rMaster", "rKey", "regular_key")])
    sigs = compute_key_signer_signals(store)
    assert any(s.signal_type == "regular_key" and {s.a, s.b} == {"rMaster", "rKey"} for s in sigs)

def test_shared_regular_key_links_two_accounts(store):
    _seed_edges(store, [("rA", "rKey", "regular_key"), ("rB", "rKey", "regular_key")])
    sigs = compute_key_signer_signals(store)
    shared = [s for s in sigs if s.signal_type == "shared_regular_key"]
    assert len(shared) == 1 and {shared[0].a, shared[0].b} == {"rA", "rB"}
    assert shared[0].strength == 1.0

def _private(store, *addrs):
    for a in addrs:
        store.upsert_account(a, is_service_leaf=0, crawl_status="done")

def test_activation_signal(store):
    _private(store, "rFunder", "rChild")
    _seed_edges(store, [("rFunder", "rChild", "activation")])
    sigs = compute_funding_signals(store)
    assert any(s.signal_type == "activation" and {s.a, s.b} == {"rFunder", "rChild"} for s in sigs)

def test_bidirectional_self_transfer(store):
    _private(store, "rA", "rB")
    _seed_edges(store, [("rA", "rB", "payment"), ("rB", "rA", "payment")])
    sigs = compute_funding_signals(store)
    st = [s for s in sigs if s.signal_type == "self_transfer"]
    assert len(st) == 1 and {st[0].a, st[0].b} == {"rA", "rB"}

def test_counterparty_jaccard(store):
    _private(store, "rA", "rB")
    for cp in ["rX", "rY", "rZ"]:
        store.upsert_account(cp, is_service_leaf=0)
        store.record_counterparty("rA", cp)
        store.record_counterparty("rB", cp)
    sigs = compute_counterparty_nft_signals(store, min_jaccard=0.3, min_shared=3)
    assert any(s.signal_type == "counterparty_jaccard" and {s.a, s.b} == {"rA", "rB"} for s in sigs)

def test_nft_flow_signal(store):
    _private(store, "rA", "rB")
    _seed_edges(store, [("rA", "rB", "nft_transfer")])
    sigs = compute_counterparty_nft_signals(store)
    assert any(s.signal_type == "nft_flow" and {s.a, s.b} == {"rA", "rB"} for s in sigs)

def test_domain_reuse(store):
    store.upsert_account("rA", domain="6578616d706c65", is_service_leaf=0)
    store.upsert_account("rB", domain="6578616d706c65", is_service_leaf=0)
    sigs = compute_behavioral_signals(store)
    assert any(s.signal_type == "domain_reuse" and {s.a, s.b} == {"rA", "rB"} for s in sigs)


def _insert_txs(store, sender, count, close_time, start_idx=0):
    """Insert `count` transactions for `sender` all at the same close_time."""
    for i in range(count):
        tx = ParsedTx(
            tx_hash=f"TX_{sender}_{start_idx + i}",
            ledger_index=start_idx + i,
            close_time=close_time,
            tx_type="Payment",
            sender=sender,
            destination=None,
            amount=None,
            currency=None,
            issuer=None,
            fee="10",
            result="tesSUCCESS",
            edges=[],
        )
        store.insert_transaction(tx)


def test_active_hours_signal(store):
    # Two private accounts with identical hour distributions (cosine = 1.0).
    # close_time=3600 → hour = (3600 + 946684800) // 3600 % 24 = 1; single-bucket spike.
    # Must be non-zero: the storage filter skips rows where close_time is falsy.
    _private(store, "rAlice", "rBob")
    _insert_txs(store, "rAlice", 20, close_time=3600, start_idx=0)
    _insert_txs(store, "rBob",   20, close_time=3600, start_idx=20)

    sigs = compute_behavioral_signals(store)
    active = [s for s in sigs if s.signal_type == "active_hours"]
    assert any({s.a, s.b} == {"rAlice", "rBob"} for s in active), (
        "expected active_hours PairSignal for rAlice/rBob"
    )

    # Negative case: third account with < 20 txs must not appear in any active_hours pair
    _private(store, "rCharlie")
    _insert_txs(store, "rCharlie", 5, close_time=3600, start_idx=40)

    sigs2 = compute_behavioral_signals(store)
    charlie_pairs = [
        s for s in sigs2
        if s.signal_type == "active_hours" and "rCharlie" in {s.a, s.b}
    ]
    assert not charlie_pairs, "rCharlie has < 20 txs and must not appear in active_hours pairs"
