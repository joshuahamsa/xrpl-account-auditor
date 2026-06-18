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
