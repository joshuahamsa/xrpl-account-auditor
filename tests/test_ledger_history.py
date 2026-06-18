from xrpl_audit.ledger_client import _is_full_history

def test_full_history_exact_genesis():
    assert _is_full_history("32570-99999999") is True

def test_full_history_below_genesis():
    assert _is_full_history("2-99999999") is True

def test_full_history_single_genesis():
    assert _is_full_history("32570-32570") is True

def test_not_full_history_above_genesis():
    assert _is_full_history("40000000-50000000") is False

def test_not_full_history_empty():
    assert _is_full_history("") is False

def test_not_full_history_garbage():
    assert _is_full_history("garbage") is False

def test_full_history_gap_range_at_genesis():
    # First segment starts at 32569 which is <= 32570 (GENESIS_LEDGER)
    assert _is_full_history("32569,32571-99999999") is True

def test_not_full_history_gap_range_above_genesis():
    # First segment starts at 40000000 which is > 32570
    assert _is_full_history("40000000,40000005-50000000") is False
