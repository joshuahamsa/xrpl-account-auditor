import math
from collections import defaultdict
from .models import PairSignal
from .storage import Store

def _pair(x: str, y: str) -> tuple[str, str]:
    return (x, y) if x <= y else (y, x)

def _edges_by_type(store: Store, edge_type: str):
    # Query by indexed column (idx_edges_type) instead of scanning every edge
    # into Python once per type.
    return [dict(r) for r in store.conn.execute(
        "SELECT * FROM edges WHERE edge_type=?", (edge_type,))]

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
                                     min_shared: int = 3,
                                     max_holders: int = 50) -> list[PairSignal]:
    out: list[PairSignal] = []
    private = _private_accounts(store)
    sets = _counterparty_sets(store, private)

    # Inverted index: counterparty -> accounts that touched it. Two accounts can
    # only have non-zero Jaccard if they co-occur under some counterparty, so we
    # generate candidate pairs from this index instead of enumerating all n^2
    # account pairs. Counterparties held by more than `max_holders` accounts are
    # hubs (everyone touches them) — non-discriminating and an O(holders^2)
    # candidate bomb — so we skip them, mirroring the crawler's --degree-cap.
    holders: dict[str, list[str]] = defaultdict(list)
    for acct, cps in sets.items():
        for cp in cps:
            holders[cp].append(acct)

    candidates: set[tuple[str, str]] = set()
    for cp, accts in holders.items():
        if len(accts) > max_holders:
            continue
        accts = sorted(set(accts))
        for i in range(len(accts)):
            for j in range(i + 1, len(accts)):
                candidates.add((accts[i], accts[j]))

    for a, b in candidates:
        sa, sb = sets[a], sets[b]
        inter = sa & sb
        if len(inter) < min_shared:
            continue
        union = sa | sb
        jac = len(inter) / len(union)
        if jac >= min_jaccard:
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

def _hour_histograms(store: Store, accounts: set[str]) -> dict[str, list[int]]:
    hist: dict[str, list[int]] = defaultdict(lambda: [0] * 24)
    for r in store.conn.execute("SELECT sender, close_time FROM transactions"):
        s = r["sender"]
        if s in accounts and r["close_time"]:
            hour = ((r["close_time"] + RIPPLE_EPOCH_OFFSET) // 3600) % 24
            hist[s][hour] += 1
    return hist

def _cosine(u: list[int], v: list[int]) -> float:
    dot = sum(a * b for a, b in zip(u, v))
    nu = math.sqrt(sum(a * a for a in u))
    nv = math.sqrt(sum(b * b for b in v))
    return dot / (nu * nv) if nu and nv else 0.0

def compute_behavioral_signals(store: Store, max_domain_holders: int = 50) -> list[PairSignal]:
    out: list[PairSignal] = []

    by_domain: dict[str, list[str]] = defaultdict(list)
    for a in store.iter_accounts():
        if a["domain"] and not a["is_service_leaf"]:
            by_domain[a["domain"]].append(a["address"])
    for domain, addrs in by_domain.items():
        addrs = sorted(set(addrs))
        # A domain shared by hundreds of accounts is a hosting/parking domain, not
        # a shared-operator signal — and an O(n^2) candidate bomb. Skip it.
        if len(addrs) > max_domain_holders:
            continue
        for i in range(len(addrs)):
            for j in range(i + 1, len(addrs)):
                a, b = _pair(addrs[i], addrs[j])
                out.append(PairSignal(a, b, "domain_reuse", 0.5, {"domain": domain}))
    return out

def compute_active_hours_signals(store: Store,
                                 candidate_pairs) -> list[PairSignal]:
    """Activity-hour cosine similarity, computed ONLY for already-nominated pairs.

    active_hours (weight 0.2) can never form a cluster link on its own — it only
    corroborates pairs another signal already surfaced. Restricting it to candidate
    pairs is what keeps clustering from materialising the ~n^2 coincidental-schedule
    matches that blew the process to 42 GB at 17k accounts.
    """
    cands = {_pair(a, b) for (a, b) in candidate_pairs}
    if not cands:
        return []
    private = _private_accounts(store)
    needed = {x for pair in cands for x in pair} & private
    if not needed:
        return []
    hist = _hour_histograms(store, needed)

    out: list[PairSignal] = []
    for a, b in cands:
        ha, hb = hist.get(a), hist.get(b)
        if ha is None or hb is None:
            continue
        if sum(ha) < 20 or sum(hb) < 20:
            continue
        cos = _cosine(ha, hb)
        if cos >= 0.9:
            out.append(PairSignal(a, b, "active_hours", 0.2, {"cosine": round(cos, 3)}))
    return out
