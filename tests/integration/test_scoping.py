import os
import uuid

import pytest
from elasticsearch import AsyncElasticsearch
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.conformance import checkpointer_test, validate

from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver

pytestmark = pytest.mark.integration


@pytest.fixture
async def saver(es_client, index_prefix):
    s = AsyncElasticsearchSaver(
        es_client,
        checkpoints_index=f"{index_prefix}checkpoints",
        writes_index=f"{index_prefix}writes",
        scoping=True,
    )
    await s.setup()
    return s


def _cfg(tenant, user="u1", thread="t1", checkpoint_id=None):
    c = {"thread_id": thread, "checkpoint_ns": "", "tenant_id": tenant, "user_id": user}
    if checkpoint_id:
        c["checkpoint_id"] = checkpoint_id
    return {"configurable": c}


async def _seed(saver, tenant, checkpoint_id, channel_value):
    cp = empty_checkpoint()
    cp["id"] = checkpoint_id
    cfg = await saver.aput(_cfg(tenant), cp, {"source": "loop", "step": 0}, {})
    await saver.aput_writes(cfg, [("ch", channel_value)], "task1")
    return cfg


async def test_two_tenants_same_thread_are_isolated(saver):
    same_id = "00000001-0000-0000-0000-000000000000"
    a_cfg = await _seed(saver, "acme", same_id, "acme-value")
    g_cfg = await _seed(saver, "globex", same_id, "globex-value")
    await _seed(saver, "globex", "00000002-0000-0000-0000-000000000000", "globex-2")

    assert a_cfg["configurable"]["tenant_id"] == "acme"

    for tenant, value in (("acme", "acme-value"), ("globex", "globex-value")):
        got = await saver.aget_tuple(_cfg(tenant, checkpoint_id=same_id))
        assert got.pending_writes == [("task1", "ch", value)]
        assert got.config["configurable"]["tenant_id"] == tenant

    latest = await saver.aget_tuple(_cfg("acme"))
    assert latest.config["configurable"]["checkpoint_id"] == same_id

    acme = [t async for t in saver.alist(_cfg("acme"))]
    assert len(acme) == 1
    assert acme[0].pending_writes == [("task1", "ch", "acme-value")]
    globex = [t async for t in saver.alist(_cfg("globex"))]
    assert len(globex) == 2
    assert g_cfg["configurable"]["tenant_id"] == "globex"

    await saver.adelete_scoped_thread("t1", "acme", "u1")
    assert await saver.aget_tuple(_cfg("acme", checkpoint_id=same_id)) is None
    assert [t async for t in saver.alist(_cfg("acme"))] == []
    survivor = await saver.aget_tuple(_cfg("globex", checkpoint_id=same_id))
    assert survivor.pending_writes == [("task1", "ch", "globex-value")]
    assert len([t async for t in saver.alist(_cfg("globex"))]) == 2


async def test_other_user_same_tenant_is_isolated(saver):
    cp_id = "00000001-0000-0000-0000-000000000000"
    cp = empty_checkpoint()
    cp["id"] = cp_id
    await saver.aput(_cfg("acme", "u1"), cp, {}, {})
    assert await saver.aget_tuple(_cfg("acme", "u2", checkpoint_id=cp_id)) is None
    assert await saver.aget_tuple(_cfg("acme", "u2")) is None


async def test_scoped_delete_works_on_unscoped_instance(es_client, index_prefix, saver):
    cp = empty_checkpoint()
    cp["id"] = "00000001-0000-0000-0000-000000000000"
    await saver.aput(_cfg("acme"), cp, {}, {})
    plain = AsyncElasticsearchSaver(
        es_client,
        checkpoints_index=saver.checkpoints_index,
        writes_index=saver.writes_index,
    )
    await plain.adelete_scoped_thread("t1", "acme", "u1")
    assert await saver.aget_tuple(_cfg("acme")) is None


class _FixedIdentitySaver(AsyncElasticsearchSaver):
    """Test-only: injects a fixed tenant/user into configs that lack them."""

    def _identity(self, config):
        configurable = {
            "tenant_id": "acme",
            "user_id": "u1",
            **(config or {}).get("configurable", {}),
        }
        return super()._identity({"configurable": configurable})


@checkpointer_test(name="ScopedAsyncElasticsearchSaver")
async def scoped_es_saver():
    client = AsyncElasticsearch(os.environ.get("ES_URL", "http://localhost:9200"))
    prefix = f"test_{uuid.uuid4().hex[:8]}_"
    cp_index, w_index = f"{prefix}checkpoints", f"{prefix}writes"
    try:
        saver = _FixedIdentitySaver(
            client, checkpoints_index=cp_index, writes_index=w_index, scoping=True
        )
        await saver.setup()
        yield saver
    finally:
        await client.indices.delete(index=[cp_index, w_index], ignore_unavailable=True)
        await client.close()


async def test_scoped_conformance(es_client):
    report = await validate(scoped_es_saver)
    report.print_report()
    assert report.passed_all_base()
