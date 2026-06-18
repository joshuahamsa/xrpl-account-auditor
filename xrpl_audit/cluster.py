import json
from collections import defaultdict
from .models import PairSignal, Cluster
from .storage import Store
from .signals import (compute_key_signer_signals, compute_funding_signals,
                      compute_counterparty_nft_signals, compute_behavioral_signals)

DEFAULT_WEIGHTS = {
    "regular_key": 1.0, "shared_regular_key": 1.0, "signer_list": 1.0,
    "activation": 1.0, "self_transfer": 1.0,
    "counterparty_jaccard": 1.0, "nft_flow": 1.0,
    "domain_reuse": 1.0, "active_hours": 1.0,
}

def score_pairs(signals, weights=None):
    weights = weights or DEFAULT_WEIGHTS
    acc: dict[tuple[str, str], dict] = defaultdict(lambda: {"score": 0.0, "signals": []})
    for s in signals:
        key = (s.a, s.b) if s.a <= s.b else (s.b, s.a)
        w = weights.get(s.signal_type, 1.0)
        acc[key]["score"] = round(acc[key]["score"] + s.strength * w, 6)
        acc[key]["signals"].append(s)
    return dict(acc)

def assign_tier(score: float) -> str | None:
    if score >= 1.0:
        return "confirmed"
    if score >= 0.6:
        return "likely"
    if score >= 0.3:
        return "possible"
    return None

class _UF:
    def __init__(self): self.p = {}
    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]; x = self.p[x]
        return x
    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb: self.p[ra] = rb

def build_clusters(pair_scores, link_threshold: float = 0.6) -> list[Cluster]:
    uf = _UF()
    for (a, b), info in pair_scores.items():
        if info["score"] >= link_threshold:
            uf.union(a, b)
    groups: dict[str, set] = defaultdict(set)
    evidence: dict[str, list] = defaultdict(list)
    best: dict[str, float] = defaultdict(float)
    for (a, b), info in pair_scores.items():
        if info["score"] >= link_threshold:
            root = uf.find(a)
            groups[root] |= {a, b}
            evidence[root].extend(info["signals"])
            best[root] = max(best[root], info["score"])
    clusters = []
    for i, (root, members) in enumerate(sorted(groups.items()), start=1):
        clusters.append(Cluster(id=i, members=members,
                                tier=assign_tier(best[root]) or "possible",
                                evidence=evidence[root]))
    return clusters

def run_clustering(store: Store, weights=None) -> list[Cluster]:
    signals = (compute_key_signer_signals(store)
               + compute_funding_signals(store)
               + compute_counterparty_nft_signals(store)
               + compute_behavioral_signals(store))
    store.conn.execute("DELETE FROM signals")
    store.conn.execute("DELETE FROM clusters")
    for s in signals:
        store.conn.execute("INSERT INTO signals(a,b,signal_type,strength,detail) VALUES (?,?,?,?,?)",
                           (s.a, s.b, s.signal_type, s.strength, json.dumps(s.detail)))
    scored = score_pairs(signals, weights)
    clusters = build_clusters(scored)
    for c in clusters:
        ev = json.dumps([{"a": s.a, "b": s.b, "type": s.signal_type,
                          "strength": s.strength, "detail": s.detail} for s in c.evidence])
        for m in c.members:
            store.conn.execute("INSERT INTO clusters(cluster_id, member, tier, evidence) VALUES (?,?,?,?)",
                               (c.id, m, c.tier, ev))
    store.conn.commit()
    return clusters

def load_clusters(store: Store) -> list[Cluster]:
    rows = store.conn.execute("SELECT cluster_id, member, tier, evidence FROM clusters").fetchall()
    by_id: dict[int, Cluster] = {}
    for r in rows:
        c = by_id.get(r["cluster_id"])
        if c is None:
            raw = json.loads(r["evidence"]) if r["evidence"] else []
            ev = [PairSignal(a=e["a"], b=e["b"], signal_type=e["type"],
                             strength=e["strength"], detail=e["detail"]) for e in raw]
            c = Cluster(id=r["cluster_id"], members=set(), tier=r["tier"], evidence=ev)
            by_id[r["cluster_id"]] = c
        c.members.add(r["member"])
    return list(by_id.values())
