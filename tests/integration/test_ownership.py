import asyncio

import pytest

from langgraph_checkpoint_elasticsearch import SessionOwnershipStore

pytestmark = pytest.mark.integration


@pytest.fixture
async def store(es_client, index_prefix):
    s = SessionOwnershipStore(es_client, index=f"{index_prefix}owners")
    await s.setup()
    await s.setup()  # idempotent
    return s


async def test_concurrent_claims_have_one_winner(store):
    users = [f"u{i}" for i in range(10)]
    results = await asyncio.gather(*(store.claim_session("t1", "acme", u) for u in users))
    assert results.count(True) == 1
    winner = users[results.index(True)]
    assert await store.get_session_owner("t1") == ("acme", winner)


async def test_claim_and_lookup(store):
    assert await store.get_session_owner("nope") is None
    assert await store.claim_session("t2", "acme", "u1") is True
    assert await store.claim_session("t2", "acme", "u1") is True
    assert await store.claim_session("t2", "acme", "u2") is False
