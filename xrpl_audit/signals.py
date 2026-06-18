import math
from collections import defaultdict
from .models import PairSignal
from .storage import Store

def _pair(x: str, y: str) -> tuple[str, str]:
    return (x, y) if x <= y else (y, x)

def _edges_by_type(store: Store, edge_type: str):
    return [e for e in store.iter_edges() if e["edge_type"] == edge_type]

def compute_key_signer_signals(store: Store) -> list[PairSignal]:
    out: list[PairSignal] = []
    key_to_setters: dict[str, list[str]] = defaultdict(list)

    for e in _edges_by_type(store, "regular_key"):
        a, b = _pair(e["src"], e["dst"])
        out.append(PairSignal(a, b, "regular_key", 1.0, {"tx": e["tx_hash"]}))
        key_to_setters[e["dst"]].append(e["src"])

    for e in _edges_by_type(store, "signer_list"):
        a, b = _pair(e["src"], e["dst"])
        out.append(PairSignal(a, b, "signer_list", 0.9, {"tx": e["tx_hash"]}))

    for key, setters in key_to_setters.items():
        uniq = sorted(set(setters))
        for i in range(len(uniq)):
            for j in range(i + 1, len(uniq)):
                a, b = _pair(uniq[i], uniq[j])
                out.append(PairSignal(a, b, "shared_regular_key", 1.0, {"key": key}))
    return out

def _private_accounts(store: Store) -> set[str]:
    return {a["address"] for a in store.iter_accounts() if not a["is_service_leaf"]}

def _counterparty_sets(store: Store, private: set[str]) -> dict[str, set[str]]:
    sets: dict[str, set[str]] = defaultdict(set)
    rows = store.conn.execute("SELECT address, counterparty FROM counterparties")
    for r in rows:
        if r["address"] in private and r["counterparty"] in private:
            sets[r["address"]].add(r["counterparty"])
    return sets

def compute_counterparty_nft_signals(store: Store, min_jaccard: float = 0.3,
                                     min_shared: int = 3) -> list[PairSignal]:
    out: list[PairSignal] = []
    private = _private_accounts(store)
    sets = _counterparty_sets(store, private)
    accts = sorted(sets)
    for i in range(len(accts)):
        for j in range(i + 1, len(accts)):
            a, b = accts[i], accts[j]
            sa, sb = sets[a], sets[b]
            inter = sa & sb
            union = sa | sb
            if not union:
                continue
            jac = len(inter) / len(union)
            if jac >= min_jaccard and len(inter) >= min_shared:
                out.append(PairSignal(a, b, "counterparty_jaccard", min(0.5, jac),
                                      {"jaccard": round(jac, 3), "shared": len(inter)}))
    for etype in ("nft_transfer", "nft_sale"):
        for e in _edges_by_type(store, etype):
            if e["src"] in private and e["dst"] in private:
                a, b = _pair(e["src"], e["dst"])
                out.append(PairSignal(a, b, "nft_flow", 0.4, {"edge": etype, "tx": e["tx_hash"]}))
    return out

def compute_funding_signals(store: Store) -> list[PairSignal]:
    out: list[PairSignal] = []
    private = _private_accounts(store)

    for e in _edges_by_type(store, "activation"):
        a, b = _pair(e["src"], e["dst"])
        out.append(PairSignal(a, b, "activation", 0.7, {"tx": e["tx_hash"]}))

    directed: set[tuple[str, str]] = set()
    for e in _edges_by_type(store, "payment"):
        if e["src"] in private and e["dst"] in private:
            directed.add((e["src"], e["dst"]))
    emitted: set[tuple[str, str]] = set()
    for s, d in directed:
        if (d, s) in directed:
            a, b = _pair(s, d)
            if (a, b) not in emitted:
                emitted.add((a, b))
                out.append(PairSignal(a, b, "self_transfer", 0.6, {}))
    return out

RIPPLE_EPOCH_OFFSET = 946684800

def _hour_histograms(store: Store, private: set[str]) -> dict[str, list[int]]:
    hist: dict[str, list[int]] = defaultdict(lambda: [0] * 24)
    for r in store.conn.execute("SELECT sender, close_time FROM transactions"):
        s = r["sender"]
        if s in private and r["close_time"]:
            hour = ((r["close_time"] + RIPPLE_EPOCH_OFFSET) // 3600) % 24
            hist[s][hour] += 1
    return hist

def _cosine(u: list[int], v: list[int]) -> float:
    dot = sum(a * b for a, b in zip(u, v))
    nu = math.sqrt(sum(a * a for a in u))
    nv = math.sqrt(sum(b * b for b in v))
    return dot / (nu * nv) if nu and nv else 0.0

def compute_behavioral_signals(store: Store) -> list[PairSignal]:
    out: list[PairSignal] = []
    private = _private_accounts(store)

    by_domain: dict[str, list[str]] = defaultdict(list)
    for a in store.iter_accounts():
        if a["domain"] and not a["is_service_leaf"]:
            by_domain[a["domain"]].append(a["address"])
    for domain, addrs in by_domain.items():
        addrs = sorted(set(addrs))
        for i in range(len(addrs)):
            for j in range(i + 1, len(addrs)):
                a, b = _pair(addrs[i], addrs[j])
                out.append(PairSignal(a, b, "domain_reuse", 0.5, {"domain": domain}))

    hist = _hour_histograms(store, private)
    accts = sorted(k for k, v in hist.items() if sum(v) >= 20)
    for i in range(len(accts)):
        for j in range(i + 1, len(accts)):
            a, b = accts[i], accts[j]
            cos = _cosine(hist[a], hist[b])
            if cos >= 0.9:
                out.append(PairSignal(a, b, "active_hours", 0.2, {"cosine": round(cos, 3)}))
    return out
