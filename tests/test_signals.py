from xrpl_audit.models import Edge
from xrpl_audit.signals import compute_key_signer_signals

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
