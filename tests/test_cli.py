from click.testing import CliRunner
from xrpl_audit.cli import cli
from xrpl_audit.storage import Store

def test_status_reports_counts(tmp_path):
    db = tmp_path / "audit.db"
    s = Store(str(db)); s.init_schema()
    s.upsert_account("rA", crawl_status="done")
    s.conn.close()
    res = CliRunner().invoke(cli, ["--db", str(db), "status"])
    assert res.exit_code == 0
    assert "accounts" in res.output and "1" in res.output
