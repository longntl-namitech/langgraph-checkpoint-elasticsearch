from typing import TypedDict

import pytest
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.graph import END, START, StateGraph

from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver

pytestmark = pytest.mark.integration


@pytest.fixture
async def saver(es_client, index_prefix):
    s = AsyncElasticsearchSaver(
        es_client,
        checkpoints_index=f"{index_prefix}checkpoints",
        writes_index=f"{index_prefix}writes",
    )
    await s.setup()
    return s


def _cfg(thread="t1", ns="", checkpoint_id=None):
    c = {"thread_id": thread, "checkpoint_ns": ns}
    if checkpoint_id:
        c["checkpoint_id"] = checkpoint_id
    return {"configurable": c}


def _cp(i):
    cp = empty_checkpoint()
    cp["id"] = f"{i:08d}-0000-0000-0000-000000000000"
    return cp


async def test_setup_is_idempotent(saver):
    await saver.setup()


async def test_put_then_get_roundtrip(saver):
    cp = _cp(1)
    cp["channel_values"] = {"k": "v"}
    md = {"source": "input", "step": -1, "parents": {}}
    out = await saver.aput(_cfg(), cp, md, {})
    assert out == _cfg(checkpoint_id=cp["id"])
    tup = await saver.aget_tuple(_cfg())
    assert tup.checkpoint == cp
    assert tup.metadata == md
    assert tup.config == out
    assert tup.parent_config is None


async def test_latest_visible_immediately_after_put(saver):
    cfg = _cfg()
    for i in (1, 2, 3):
        cfg = await saver.aput(cfg, _cp(i), {}, {})
    tup = await saver.aget_tuple(_cfg())
    assert tup.checkpoint["id"] == _cp(3)["id"]


async def test_root_latest_ignores_subgraph_namespace(saver):
    await saver.aput(_cfg(), _cp(1), {}, {})
    await saver.aput(_cfg(ns="child:1"), _cp(2), {}, {})
    tup = await saver.aget_tuple(_cfg(ns=""))
    assert tup.checkpoint["id"] == _cp(1)["id"]


async def test_alist_returns_all_beyond_one_page(saver):
    cfg = _cfg()
    saver.refresh = False  # bulk load; wait_for would cost ~1s per put
    for i in range(250):
        cfg = await saver.aput(cfg, _cp(i), {}, {})
    await saver.client.indices.refresh(index=saver.checkpoints_index)
    ids = [t.checkpoint["id"] async for t in saver.alist(_cfg())]
    assert len(ids) == 250
    assert ids == sorted(ids, reverse=True)
    assert len(set(ids)) == 250


async def test_alist_filter_matches_metadata_fields(saver):
    await saver.aput(_cfg(), _cp(1), {"source": "loop", "step": 1}, {})
    await saver.aput(_cfg(), _cp(2), {"source": "input", "step": -1}, {})
    found = [t async for t in saver.alist(_cfg(), filter={"source": "loop", "step": 1})]
    assert [t.checkpoint["id"] for t in found] == [_cp(1)["id"]]


async def test_pending_writes_attached_and_ordered(saver):
    cfg = await saver.aput(_cfg(), _cp(1), {}, {})
    await saver.aput_writes(cfg, [("a", 1), ("b", 2)], "task-b")
    await saver.aput_writes(cfg, [("a", 3), ("b", 4)], "task-a")
    await saver.aput_writes(cfg, [("__error__", "boom")], "task-a")
    await saver.aput_writes(cfg, [("__error__", "boom")], "task-a")
    # first write wins for regular channels
    await saver.aput_writes(cfg, [("a", 99)], "task-a")
    tup = await saver.aget_tuple(_cfg())
    assert tup.pending_writes == [
        ("task-a", "__error__", "boom"),  # special channels have negative idx
        ("task-a", "a", 3),
        ("task-a", "b", 4),
        ("task-b", "a", 1),
        ("task-b", "b", 2),
    ]


async def test_adelete_thread_removes_checkpoints_and_writes(saver):
    for thread in ("t1", "t2"):
        cfg = await saver.aput(_cfg(thread), _cp(1), {}, {})
        await saver.aput_writes(cfg, [("a", 1)], "task")
    await saver.adelete_thread("t1")
    assert await saver.aget_tuple(_cfg("t1")) is None
    other = await saver.aget_tuple(_cfg("t2"))
    assert other is not None and other.pending_writes == [("task", "a", 1)]
    res = await saver.client.count(index=saver.writes_index)
    assert res["count"] == 1


class State(TypedDict):
    items: list[str]


async def test_graph_resume_with_subgraph(es_client, index_prefix):
    def make():
        return AsyncElasticsearchSaver(
            es_client,
            checkpoints_index=f"{index_prefix}checkpoints",
            writes_index=f"{index_prefix}writes",
        )

    async def inner(state: State):
        return {"items": [*state["items"], "inner"]}

    async def outer(state: State):
        return {"items": [*state["items"], "outer"]}

    sub = StateGraph(State)
    sub.add_node("inner", inner)
    sub.add_edge(START, "inner")
    sub.add_edge("inner", END)

    def build(saver):
        g = StateGraph(State)
        g.add_node("sub", sub.compile())
        g.add_node("outer", outer)
        g.add_edge(START, "sub")
        g.add_edge("sub", "outer")
        g.add_edge("outer", END)
        return g.compile(checkpointer=saver)

    first = make()
    await first.setup()
    cfg = {"configurable": {"thread_id": "g1"}}
    r1 = await build(first).ainvoke({"items": ["a"]}, cfg)
    assert r1["items"] == ["a", "inner", "outer"]
    app2 = build(make())
    assert (await app2.aget_state(cfg)).values["items"] == ["a", "inner", "outer"]
    r2 = await app2.ainvoke({"items": ["b"]}, cfg)
    assert r2["items"] == ["b", "inner", "outer"]
    history = [s async for s in app2.aget_state_history(cfg)]
    assert len(history) > 4


async def test_sync_methods_raise(saver):
    with pytest.raises(NotImplementedError, match="async only"):
        saver.get_tuple(_cfg())
