from click.testing import CliRunner
from xrpl_audit.cli import cli
from xrpl_audit.storage import Store
from xrpl_audit.models import Edge

def test_status_reports_counts(tmp_path):
    db = tmp_path / "audit.db"
    s = Store(str(db)); s.init_schema()
    s.upsert_account("rA", crawl_status="done")
    s.conn.close()
    res = CliRunner().invoke(cli, ["--db", str(db), "status"])
    assert res.exit_code == 0
    assert "accounts" in res.output and "1" in res.output

def test_cluster_command_finds_shared_key_cluster(tmp_path):
    db = tmp_path / "audit.db"
    s = Store(str(db)); s.init_schema()
    for a in ("rA", "rB"):
        s.upsert_account(a, is_service_leaf=0, crawl_status="done")
        s.insert_edge(Edge(a, "rKey", "regular_key"), tx_hash=f"H{a}", ledger_index=1)
    s.conn.close()
    res = CliRunner().invoke(cli, ["--db", str(db), "cluster"])
    assert res.exit_code == 0
    assert "confirmed" in res.output

def test_report_obsidian(tmp_path):
    db = tmp_path / "audit.db"
    s = Store(str(db)); s.init_schema()
    s.upsert_account("rA", is_service_leaf=0, crawl_status="done")
    s.conn.execute("INSERT INTO clusters(cluster_id, member, tier, evidence) VALUES (1,'rA','confirmed','[]')")
    s.conn.commit(); s.conn.close()
    vault = tmp_path / "vault"
    res = CliRunner().invoke(cli, ["--db", str(db), "report", "--format", "obsidian", "--vault", str(vault)])
    assert res.exit_code == 0
    assert (vault / "accounts" / "rA.md").exists()
