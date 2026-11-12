import pytest

from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver


class FakeClient:
    def __init__(self, dbq=None, bulk=None):
        self.dbq = list(dbq or [])
        self.bulk_response = bulk
        self.calls = []

    async def delete_by_query(self, **kwargs):
        self.calls.append(kwargs)
        return self.dbq.pop(0) if self.dbq else {"version_conflicts": 0}

    async def bulk(self, **kwargs):
        return self.bulk_response


@pytest.mark.parametrize(
    "args", [("", "a", "u"), ("t", None, "u"), ("t", "a", ""), ("t", "a", None)]
)
async def test_scoped_delete_rejects_empty_ids(args):
    client = FakeClient()
    saver = AsyncElasticsearchSaver(client, scoping=True)
    with pytest.raises(ValueError):
        await saver.adelete_scoped_thread(*args)
    assert client.calls == []


async def test_delete_retries_once_on_conflicts():
    client = FakeClient(dbq=[{"version_conflicts": 2}, {"version_conflicts": 0}])
    await AsyncElasticsearchSaver(client).adelete_thread("t1")
    assert len(client.calls) == 3  # checkpoints twice, writes once


async def test_delete_raises_when_conflicts_persist():
    client = FakeClient(dbq=[{"version_conflicts": 3}, {"version_conflicts": 3}])
    saver = AsyncElasticsearchSaver(client, checkpoints_index="cps")
    with pytest.raises(RuntimeError, match=r"cps.*3|3.*cps"):
        await saver.adelete_thread("t1")


async def test_aput_writes_error_lists_status_and_type_capped_at_five():
    items = [
        {"create": {"status": 400, "error": {"type": f"mapper_parsing_exception_{i}"}}}
        for i in range(7)
    ]
    client = FakeClient(bulk={"errors": True, "items": items})
    saver = AsyncElasticsearchSaver(client)
    cfg = {"configurable": {"thread_id": "t", "checkpoint_ns": "", "checkpoint_id": "c"}}
    with pytest.raises(RuntimeError) as exc:
        await saver.aput_writes(cfg, [("ch", i) for i in range(7)], "task")
    msg = str(exc.value)
    assert "status 400: mapper_parsing_exception_0" in msg
    assert "mapper_parsing_exception_4" in msg
    assert "mapper_parsing_exception_5" not in msg
    assert "first 5 of 7" in msg
