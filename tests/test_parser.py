import json, pathlib
from xrpl_audit.parser import parse_transaction
from xrpl_audit.models import Edge

FIX = json.loads((pathlib.Path(__file__).parent / "fixtures/account_tx_sample.json").read_text())

def test_activation_payment_produces_activation_and_payment_edges():
    p = parse_transaction(FIX["activation"])
    assert p.tx_hash == "ACT1" and p.sender == "rFunder" and p.destination == "rNewbie"
    types = {e.edge_type for e in p.edges}
    assert "activation" in types and "payment" in types
    act = next(e for e in p.edges if e.edge_type == "activation")
    assert act.src == "rFunder" and act.dst == "rNewbie"

def test_regular_key_edge():
    p = parse_transaction(FIX["regular_key"])
    assert p.tx_type == "SetRegularKey"
    assert Edge("rMaster", "rRegKey", "regular_key") in p.edges
