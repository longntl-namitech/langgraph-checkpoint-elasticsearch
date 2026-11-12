# langgraph-checkpoint-elasticsearch

An async LangGraph checkpoint saver that stores checkpoints and pending writes in Elasticsearch. Extracted from a production agent service.

## Install

Not published to PyPI.

```bash
pip install git+https://github.com/longntl-namitech/langgraph-checkpoint-elasticsearch
```

## Quickstart

```python
from elasticsearch import AsyncElasticsearch
from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver

client = AsyncElasticsearch("http://localhost:9200")
saver = AsyncElasticsearchSaver(client)
await saver.setup()  # creates the indexes if they are missing

graph = builder.compile(checkpointer=saver)
result = await graph.ainvoke({"messages": []}, {"configurable": {"thread_id": "demo"}})
```

Runnable versions are in `examples/`: `resume.py` resumes a thread with a new client and saver, `scoping.py` shows two users on one `thread_id`. Both need only a local Elasticsearch (`ES_URL`, default `http://localhost:9200`), no LLM.

Constructor arguments: `checkpoints_index` (default `langgraph_checkpoints`), `writes_index` (default `langgraph_checkpoint_writes`), `scoping`, `refresh`, `serde`.

### Write visibility and refresh

Writes use `refresh="wait_for"` by default, so a checkpoint is visible to `aget_tuple` and `alist` as soon as the write call returns. The cost is that each write call can wait up to one index refresh interval. To trade visibility for write latency, pass another value, for example `AsyncElasticsearchSaver(client, refresh=False)`. The argument is passed to Elasticsearch as the `refresh` parameter (`True`, `False` or `"wait_for"`).

## Async only

The saver supports `ainvoke`, `astream` and the other async graph methods. The sync methods (`put`, `put_writes`, `get_tuple`, `list`, `delete_thread`) raise `NotImplementedError`.

## Scoping

Scoping is off by default. With `scoping=True`, every call needs `tenant_id` and `user_id` in `config["configurable"]`:

```python
saver = AsyncElasticsearchSaver(client, scoping=True)
config = {"configurable": {"thread_id": "t1", "tenant_id": "acme", "user_id": "alice"}}
```

Checkpoints and writes are stored with both ids, and reads filter on them, so two tenants can use the same `thread_id` without seeing each other's data. A call without both ids raises `ScopeError` (a `ValueError`), including `alist(None)`.

`adelete_thread(thread_id)` deletes by `thread_id` only. With scoping on, it removes that `thread_id` for every tenant and user. Use `adelete_scoped_thread(thread_id, tenant_id, user_id)` to delete one tenant's thread.

## Session ownership

`SessionOwnershipStore` records which tenant and user own a `thread_id`, so a service can check ownership before it touches a thread.

```python
from langgraph_checkpoint_elasticsearch import SessionOwnershipStore

owners = SessionOwnershipStore(client, index="langgraph_session_owners")
await owners.setup()
claimed = await owners.claim_session("t1", "acme", "alice")  # bool
owner = await owners.get_session_owner("t1")  # (tenant_id, user_id) or None
```

## Index layout

Three indexes, all with explicit mappings created by `setup()`:

- Checkpoints (`langgraph_checkpoints`): `tenant_id`, `user_id`, `thread_id`, `checkpoint_ns`, `checkpoint_id`, `parent_checkpoint_id`, `checkpoint_type`, `metadata_type`, `checkpoint_data`, `metadata_data`, `metadata`, `created_at`.
- Writes (`langgraph_checkpoint_writes`): `tenant_id`, `user_id`, `thread_id`, `checkpoint_ns`, `checkpoint_id`, `task_id`, `task_path`, `channel`, `value_type`, `idx`, `value`.
- Session owners (`langgraph_session_owners`): `thread_id`, `tenant_id`, `user_id`, `claimed_at`.

Checkpoint, metadata and write payloads are stored in `binary` fields, serialized with the LangGraph serializer (`serde`), so state of any type that serializer supports round-trips unchanged and Elasticsearch does not try to map it. Metadata is also kept in a `flattened` field so `alist(filter=...)` can match on it.

## Conformance

The saver passes all base capabilities (`put`, `put_writes`, `get_tuple`, `list`, `delete_thread`) of the `langgraph-checkpoint-conformance` suite, 5/5, with and without scoping. The result on Elasticsearch 9.5.5 is in [docs/conformance.md](docs/conformance.md). The CI matrix runs the same tests against Elasticsearch 8.19.25 and 9.5.5.

Extended capabilities are not implemented: `copy_thread`, `delete_for_runs`, `prune`.

## Running tests locally

```bash
docker compose up -d es          # Elasticsearch 9.5.5 on localhost:9200
uv sync
uv run pytest -m "not integration"  # unit tests
uv run pytest -m integration     # needs Elasticsearch; set ES_URL if not on localhost:9200
```

For Elasticsearch 8, start it with `ES_VERSION=8.19.25 docker compose up -d es` and install the 8.x client: `uv pip install --python .venv/bin/python "elasticsearch[async]>=8,<9"`, then run `uv run --no-sync pytest -m integration`.

## Compatibility

- `langgraph>=1`, `langgraph-checkpoint>=4`, Python 3.11+.
- Elasticsearch 8.x and 9.x, with the matching client major version (the 9.x client does not talk to Elasticsearch 8).

## License

MIT
