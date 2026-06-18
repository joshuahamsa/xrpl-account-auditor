import shutil
from collections import defaultdict
from pathlib import Path
from .storage import Store
from .models import Cluster

def _account_cluster_map(clusters: list[Cluster]) -> dict[str, Cluster]:
    m: dict[str, Cluster] = {}
    for c in clusters:
        for member in c.members:
            m[member] = c
    return m

def export_obsidian(store: Store, clusters: list[Cluster], vault_dir: str) -> None:
    root = Path(vault_dir)
    accounts_dir = root / "accounts"
    clusters_dir = root / "clusters"
    for d in (accounts_dir, clusters_dir):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    cmap = _account_cluster_map(clusters)

    edges_out: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for e in store.iter_edges():
        edges_out[e["src"]][e["edge_type"]].append(e["dst"])

    for acct in store.iter_accounts():
        addr = acct["address"]
        c = cmap.get(addr)
        fm = ["---",
              f"address: {addr}",
              f"hop_depth: {acct['hop_depth']}",
              f"tx_count: {acct['tx_count']}",
              f"counterparty_count: {acct['counterparty_count']}",
              f"is_service_leaf: {bool(acct['is_service_leaf'])}",
              f"activation_parent: {acct['activation_parent'] or ''}"]
        if c:
            fm += [f"cluster: {c.id}", f"confidence: {c.tier}"]
        fm.append("---")
        body = ["\n".join(fm), ""]
        tags = []
        if acct["is_service_leaf"]:
            tags.append("#service-leaf")
        if c:
            tags += [f"#cluster/{c.id}", f"#confidence/{c.tier}"]
        if tags:
            body += [" ".join(tags), ""]
        if acct["activation_parent"]:
            body += [f"Activated by [[{acct['activation_parent']}]]", ""]
        for etype, dsts in sorted(edges_out.get(addr, {}).items()):
            body.append(f"## {etype}")
            for d in sorted(set(dsts)):
                body.append(f"- [[{d}]]")
            body.append("")
        (accounts_dir / f"{addr}.md").write_text("\n".join(body))

    for c in clusters:
        fm = ["---", f"cluster_id: {c.id}", f"tier: {c.tier}",
              f"size: {len(c.members)}", "---", ""]
        lines = fm + [f"# Cluster {c.id} ({c.tier})", "", "## Members"]
        lines += [f"- [[{m}]]" for m in sorted(c.members)]
        lines += ["", "## Evidence"]
        for s in c.evidence:
            lines.append(f"- `{s.signal_type}` [[{s.a}]] ↔ [[{s.b}]] (strength {s.strength}) {s.detail}")
        (clusters_dir / f"cluster-{c.id}.md").write_text("\n".join(lines))
