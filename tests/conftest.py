import pytest
from xrpl_audit.storage import Store

@pytest.fixture
def store():
    s = Store(":memory:")
    s.init_schema()
    return s
