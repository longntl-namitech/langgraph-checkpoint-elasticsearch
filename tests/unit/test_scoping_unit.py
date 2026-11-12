import pytest
from langgraph.checkpoint.base import empty_checkpoint

from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver, ScopeError
from langgraph_checkpoint_elasticsearch._mappings import checkpoint_doc_id


class FakeClient:
    def __init__(self):
        self.indexed = []
        self.searches = []
        self.bulks = []

    async def index(self, **kwargs):
        self.indexed.append(kwargs)

    async def bulk(self, **kwargs):
        self.bulks.append(kwargs)
        return {"errors": False, "items": []}

    async def search(self, **kwargs):
        self.searches.append(kwargs)
        return {"hits": {"hits": []}}


def _config(**extra):
    return {"configurable": {"thread_id": "t1", "checkpoint_ns": "", **extra}}


def _cp():
    cp = empty_checkpoint()
    cp["id"] = "00000001-0000-0000-0000-000000000000"
    return cp


IDENT = {"tenant_id": "acme", "user_id": "u1"}


async def test_scoping_off_ignores_identity():
    client = FakeClient()
    saver = AsyncElasticsearchSaver(client)
    out = await saver.aput(_config(**IDENT), _cp(), {}, {})
    cp = _cp()
    assert client.indexed[0]["id"] == checkpoint_doc_id("t1", "", cp["id"])
    assert "tenant_id" not in client.indexed[0]["document"]
    assert "tenant_id" not in out["configurable"]


async def test_scoped_put_uses_identity_doc_id_and_fields():
    client = FakeClient()
    saver = AsyncElasticsearchSaver(client, scoping=True)
    out = await saver.aput(_config(**IDENT), _cp(), {}, {})
    cp = _cp()
    call = client.indexed[0]
    assert call["id"] == checkpoint_doc_id("t1", "", cp["id"], "acme", "u1")
    assert call["document"]["tenant_id"] == "acme"
    assert call["document"]["user_id"] == "u1"
    assert out["configurable"]["tenant_id"] == "acme"
    assert out["configurable"]["user_id"] == "u1"


@pytest.mark.parametrize(
    "extra",
    [{"user_id": "u1"}, {"tenant_id": "acme"}, {"tenant_id": "", "user_id": "u1"}, {}],
)
@pytest.mark.parametrize("method", ["aput", "aput_writes", "aget_tuple", "alist"])
async def test_missing_identity_raises(method, extra):
    saver = AsyncElasticsearchSaver(FakeClient(), scoping=True)
    config = _config(checkpoint_id="00000001-0000-0000-0000-000000000000", **extra)
    with pytest.raises(ScopeError):
        if method == "aput":
            await saver.aput(config, _cp(), {}, {})
        elif method == "aput_writes":
            await saver.aput_writes(config, [("c", 1)], "task")
        elif method == "aget_tuple":
            await saver.aget_tuple(config)
        else:
            async for _ in saver.alist(config):
                pass


async def test_alist_without_config_raises_when_scoped():
    saver = AsyncElasticsearchSaver(FakeClient(), scoping=True)
    with pytest.raises(ScopeError):
        async for _ in saver.alist(None):
            pass


async def test_scoped_queries_filter_on_identity():
    client = FakeClient()
    saver = AsyncElasticsearchSaver(client, scoping=True)
    await saver.aget_tuple(_config(**IDENT))
    async for _ in saver.alist(_config(**IDENT)):
        pass
    assert len(client.searches) == 2
    for search in client.searches:
        filters = search["query"]["bool"]["filter"]
        assert {"term": {"tenant_id": "acme"}} in filters
        assert {"term": {"user_id": "u1"}} in filters


async def test_unscoped_queries_have_no_identity_filter():
    client = FakeClient()
    saver = AsyncElasticsearchSaver(client)
    await saver.aget_tuple(_config(**IDENT))
    filters = client.searches[0]["query"]["bool"]["filter"]
    assert not any("tenant_id" in f.get("term", {}) for f in filters)
