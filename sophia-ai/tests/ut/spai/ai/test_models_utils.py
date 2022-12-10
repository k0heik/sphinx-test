import pytest
from spai.ai.utils import serialize, deserialize


@pytest.mark.parametrize("obj", [1, (3, 5), {"a": 1}])
def test_serialization(obj):
    binary = serialize(obj)
    recon_obj = deserialize(binary)
    assert obj == recon_obj
