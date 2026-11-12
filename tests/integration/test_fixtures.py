import pytest

pytestmark = pytest.mark.integration

_created: list[str] = []


async def test_index_prefix_creates_index(es_client, index_prefix):
    name = f"{index_prefix}probe"
    await es_client.indices.create(index=name)
    _created.append(name)
    assert await es_client.indices.exists(index=name)


async def test_index_prefix_teardown_removed_index(es_client):
    # runs after the test above; its index_prefix teardown must have deleted the index
    assert _created
    assert not await es_client.indices.exists(index=_created[0])
