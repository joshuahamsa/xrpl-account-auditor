from xrpl_audit.models import Edge
from xrpl_audit.signals import compute_key_signer_signals, compute_funding_signals

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
