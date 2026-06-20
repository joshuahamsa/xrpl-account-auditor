from xrpl_audit.models import PairSignal, Edge
from xrpl_audit.cluster import score_pairs, assign_tier, build_clusters, run_clustering, load_clusters
from xrpl_audit.storage import Store

def test_score_accumulates_weighted_signals():
    sigs = [PairSignal("rA", "rB", "activation", 0.7, {}),
            PairSignal("rA", "rB", "self_transfer", 0.6, {})]
    scored = score_pairs(sigs, weights={"activation": 1.0, "self_transfer": 1.0})
    assert scored[("rA", "rB")]["score"] == 1.3

def test_assign_tier():
    assert assign_tier(1.0) == "confirmed"
    assert assign_tier(0.6) == "likely"
    assert assign_tier(0.3) == "possible"
    assert assign_tier(0.1) is None

def test_build_clusters_unions_strong_pairs():
    sigs = [PairSignal("rA", "rB", "shared_regular_key", 1.0, {}),
            PairSignal("rB", "rC", "activation", 0.7, {})]
    scored = score_pairs(sigs)
    clusters = build_clusters(scored)
    members = [c.members for c in clusters]
    assert any({"rA", "rB", "rC"} == m for m in members)
    big = next(c for c in clusters if c.members == {"rA", "rB", "rC"})
    assert big.tier == "confirmed"

def test_active_hours_does_not_explode_without_candidates():
    # 50 accounts all active in the same hour, each with >=20 txs, but NO linking
    # signal between any of them. The old all-pairs code materialised C(50,2)=1225
    # active_hours PairSignals here (the mechanism behind the 42 GB OOM at 17k
    # accounts). The candidate-gated path must write zero.
    from xrpl_audit.models import ParsedTx
    store = Store(":memory:")
    store.init_schema()
    for i in range(50):
        addr = f"rZ{i:02d}"
        store.upsert_account(addr, is_service_leaf=0, crawl_status="done")
        for j in range(20):
            store.insert_transaction(ParsedTx(
                tx_hash=f"TX_{addr}_{j}", ledger_index=i * 20 + j, close_time=3600,
                tx_type="Payment", sender=addr, destination=None, amount=None,
                currency=None, issuer=None, fee="10", result="tesSUCCESS", edges=[]))
    run_clustering(store)
    n_active = store.conn.execute(
        "SELECT COUNT(*) FROM signals WHERE signal_type='active_hours'").fetchone()[0]
    assert n_active == 0, f"active_hours exploded: {n_active} signals (expected 0)"

def test_load_clusters_restores_evidence():
    store = Store(":memory:")
    store.init_schema()
    # Seed two private accounts each with a regular_key edge to a shared key
    store.upsert_account("rA", hop_depth=0)
    store.upsert_account("rB", hop_depth=0)
    store.upsert_account("rKey", hop_depth=1, is_service_leaf=1)
    store.insert_edge(Edge(src="rA", dst="rKey", edge_type="regular_key"), tx_hash="tx1", ledger_index=1000)
    store.insert_edge(Edge(src="rB", dst="rKey", edge_type="regular_key"), tx_hash="tx2", ledger_index=1001)
    run_clustering(store)
    loaded = load_clusters(store)
    assert any(len(c.evidence) > 0 for c in loaded), "no cluster has evidence after load"
    assert any(s.signal_type for c in loaded for s in c.evidence), "no signal_type found in loaded evidence"
