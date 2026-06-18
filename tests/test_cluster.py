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
