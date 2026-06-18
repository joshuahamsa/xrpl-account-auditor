from xrpl_audit.models import PairSignal
from xrpl_audit.cluster import score_pairs, assign_tier, build_clusters

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
