import pathlib
from xrpl_audit.models import Edge, Cluster, PairSignal
from xrpl_audit.report import export_obsidian

def test_export_writes_account_and_cluster_notes(store, tmp_path):
    store.upsert_account("rA", hop_depth=0, tx_count=2, counterparty_count=1,
                         is_service_leaf=0, crawl_status="done")
    store.upsert_account("rB", hop_depth=1, is_service_leaf=0, crawl_status="done")
    store.insert_edge(Edge("rA", "rB", "payment"), tx_hash="H1", ledger_index=1)
    clusters = [Cluster(id=1, members={"rA", "rB"}, tier="confirmed",
                        evidence=[PairSignal("rA", "rB", "shared_regular_key", 1.0, {})])]
    export_obsidian(store, clusters, str(tmp_path))
    note = (tmp_path / "accounts" / "rA.md").read_text()
    assert "[[rB]]" in note and "cluster: 1" in note and "confirmed" in note
    cnote = (tmp_path / "clusters" / "cluster-1.md").read_text()
    assert "[[rA]]" in cnote and "[[rB]]" in cnote

def test_service_leaf_is_tagged(store, tmp_path):
    store.upsert_account("rExch", is_service_leaf=1, crawl_status="leaf")
    export_obsidian(store, [], str(tmp_path))
    note = (tmp_path / "accounts" / "rExch.md").read_text()
    assert "#service-leaf" in note
