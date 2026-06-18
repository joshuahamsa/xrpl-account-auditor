# tests/test_models.py
from xrpl_audit.models import ParsedTx, Edge, PairSignal, Cluster

def test_edge_is_hashable_and_equal():
    e1 = Edge(src="rA", dst="rB", edge_type="payment", metadata={})
    e2 = Edge(src="rA", dst="rB", edge_type="payment", metadata={})
    assert e1 == e2

def test_parsedtx_holds_edges():
    tx = ParsedTx(tx_hash="H", ledger_index=1, close_time=0, tx_type="Payment",
                  sender="rA", destination="rB", amount="10", currency="XRP",
                  issuer=None, fee="12", result="tesSUCCESS", edges=[])
    assert tx.tx_hash == "H" and tx.edges == []

def test_paircluster_defaults():
    ps = PairSignal(a="rA", b="rB", signal_type="regular_key", strength=1.0, detail={})
    cl = Cluster(id=1, members={"rA", "rB"}, tier="confirmed", evidence=[ps])
    assert "rA" in cl.members and cl.tier == "confirmed"
