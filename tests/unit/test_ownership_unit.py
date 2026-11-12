import pytest
from elasticsearch import ApiError, NotFoundError

from langgraph_checkpoint_elasticsearch import SessionOwnershipStore


class _Meta:
    status = 409
    headers: dict = {}


class FakeClient:
    """In-memory stand-in: create conflicts like ES, get returns the stored doc."""

    def __init__(self):
        self.docs = {}
        self.calls = []

    async def index(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["id"] in self.docs:
            raise _conflict()
        self.docs[kwargs["id"]] = kwargs["document"]

    async def get(self, **kwargs):
        if kwargs["id"] not in self.docs:
            raise NotFoundError("not found", _NotFoundMeta(), {})
        return {"_source": self.docs[kwargs["id"]]}


class _NotFoundMeta:
    status = 404
    headers: dict = {}


def _conflict():
    from elasticsearch import ConflictError

    return ConflictError("conflict", _Meta(), {})


async def test_first_claim_wins():
    client = FakeClient()
    store = SessionOwnershipStore(client)
    assert await store.claim_session("t1", "acme", "u1") is True
    call = client.calls[0]
    assert call["op_type"] == "create"
    assert call["refresh"] == "wait_for"
    assert call["index"] == "langgraph_session_owners"
    assert call["id"] == "t1"
    doc = call["document"]
    assert (doc["thread_id"], doc["tenant_id"], doc["user_id"]) == ("t1", "acme", "u1")
    assert doc["claimed_at"]


async def test_same_owner_reclaim_is_true():
    store = SessionOwnershipStore(FakeClient())
    await store.claim_session("t1", "acme", "u1")
    assert await store.claim_session("t1", "acme", "u1") is True


async def test_other_owner_is_false():
    store = SessionOwnershipStore(FakeClient())
    await store.claim_session("t1", "acme", "u1")
    assert await store.claim_session("t1", "acme", "u2") is False
    assert await store.claim_session("t1", "other", "u1") is False


async def test_get_session_owner():
    store = SessionOwnershipStore(FakeClient())
    assert await store.get_session_owner("t1") is None
    await store.claim_session("t1", "acme", "u1")
    assert await store.get_session_owner("t1") == ("acme", "u1")


async def test_doc_vanishing_after_conflict_returns_false():
    class Vanishing(FakeClient):
        async def index(self, **kwargs):
            raise _conflict()

    assert await SessionOwnershipStore(Vanishing()).claim_session("t1", "acme", "u1") is False


@pytest.mark.parametrize(
    "args",
    [
        ("t1", "", "u1"),
        ("t1", "acme", ""),
        ("t1", None, "u1"),
        ("t1", "acme", None),
        ("", "acme", "u1"),
    ],
)
async def test_empty_identity_raises_before_any_call(args):
    client = FakeClient()
    with pytest.raises(ValueError):
        await SessionOwnershipStore(client).claim_session(*args)
    assert client.calls == []


async def test_other_api_errors_propagate():
    class Boom(FakeClient):
        async def index(self, **kwargs):
            raise ApiError("boom", _Meta(), {})

    with pytest.raises(ApiError):
        await SessionOwnershipStore(Boom()).claim_session("t1", "acme", "u1")
