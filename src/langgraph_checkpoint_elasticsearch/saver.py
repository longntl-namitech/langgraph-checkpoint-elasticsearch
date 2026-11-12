from __future__ import annotations

import base64
import json
import logging
import random
from collections.abc import AsyncIterator, Sequence
from typing import Any

from elasticsearch import AsyncElasticsearch, BadRequestError, NotFoundError
from elasticsearch.exceptions import ConflictError
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    SerializerProtocol,
    get_checkpoint_id,
    get_checkpoint_metadata,
)

from langgraph_checkpoint_elasticsearch._mappings import (
    CHECKPOINTS_MAPPING,
    WRITES_MAPPING,
    checkpoint_doc_id,
    write_doc_id,
)

logger = logging.getLogger(__name__)

_LIST_PAGE_SIZE = 100
_WRITES_PAGE_SIZE = 1000
_ASYNC_ONLY = "AsyncElasticsearchSaver is async only; use ainvoke/astream"


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _term_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class AsyncElasticsearchSaver(BaseCheckpointSaver[str]):
    """Async LangGraph checkpoint saver backed by Elasticsearch."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        *,
        checkpoints_index: str = "langgraph_checkpoints",
        writes_index: str = "langgraph_checkpoint_writes",
        scoping: bool = False,
        refresh: bool | str = "wait_for",
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.client = client
        self.checkpoints_index = checkpoints_index
        self.writes_index = writes_index
        self.scoping = scoping
        self.refresh = refresh

    async def setup(self) -> None:
        """Create both indexes if they are missing."""
        for index, mapping in (
            (self.checkpoints_index, CHECKPOINTS_MAPPING),
            (self.writes_index, WRITES_MAPPING),
        ):
            if await self.client.indices.exists(index=index):
                continue
            try:
                await self.client.indices.create(index=index, mappings=mapping["mappings"])
            except BadRequestError as exc:
                # A concurrent setup() may have created it first.
                if exc.error != "resource_already_exists_exception":
                    raise

    # Identity hooks: tenant/user scoping plugs in here (no behaviour when unscoped).

    def _identity(self, config: RunnableConfig | None) -> tuple[str | None, str | None]:
        return None, None

    def _identity_filters(self, tenant_id: str | None, user_id: str | None) -> list[dict]:
        return []

    def _identity_config(self, tenant_id: str | None, user_id: str | None) -> dict[str, str]:
        return {}

    # Writes

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        configurable = config["configurable"]
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = checkpoint["id"]
        tenant_id, user_id = self._identity(config)

        cp_type, cp_bytes = self.serde.dumps_typed(checkpoint)
        merged = get_checkpoint_metadata(config, metadata)
        md_type, md_bytes = self.serde.dumps_typed(merged)

        doc: dict[str, Any] = {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": configurable.get("checkpoint_id"),
            "checkpoint_type": cp_type,
            "checkpoint_data": _b64(cp_bytes),
            "metadata_type": md_type,
            "metadata_data": _b64(md_bytes),
            "metadata": json.loads(json.dumps(merged, default=str)),
            "created_at": checkpoint.get("ts"),
        }
        if tenant_id is not None and user_id is not None:
            doc["tenant_id"] = tenant_id
            doc["user_id"] = user_id

        await self.client.index(
            index=self.checkpoints_index,
            id=checkpoint_doc_id(thread_id, checkpoint_ns, checkpoint_id, tenant_id, user_id),
            document=doc,
            refresh=self.refresh,
        )
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                **self._identity_config(tenant_id, user_id),
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        configurable = config["configurable"]
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable["checkpoint_id"]
        tenant_id, user_id = self._identity(config)

        for i, (channel, value) in enumerate(writes):
            idx = WRITES_IDX_MAP.get(channel, i)
            value_type, value_bytes = self.serde.dumps_typed(value)
            doc: dict[str, Any] = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "task_path": task_path,
                "idx": idx,
                "channel": channel,
                "value_type": value_type,
                "value": _b64(value_bytes),
            }
            if tenant_id is not None and user_id is not None:
                doc["tenant_id"] = tenant_id
                doc["user_id"] = user_id
            doc_id = write_doc_id(
                thread_id, checkpoint_ns, checkpoint_id, task_id, idx, tenant_id, user_id
            )
            if idx >= 0:
                # Regular writes are first-write-wins, like InMemorySaver.
                try:
                    await self.client.index(
                        index=self.writes_index,
                        id=doc_id,
                        document=doc,
                        op_type="create",
                        refresh=self.refresh,
                    )
                except ConflictError:
                    pass
            else:
                await self.client.index(
                    index=self.writes_index, id=doc_id, document=doc, refresh=self.refresh
                )

    # Reads

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        configurable = config["configurable"]
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = get_checkpoint_id(config)
        tenant_id, user_id = self._identity(config)

        if checkpoint_id:
            try:
                response = await self.client.get(
                    index=self.checkpoints_index,
                    id=checkpoint_doc_id(
                        thread_id, checkpoint_ns, checkpoint_id, tenant_id, user_id
                    ),
                )
            except NotFoundError:
                return None
            source = response["_source"]
        else:
            response = await self.client.search(
                index=self.checkpoints_index,
                query={
                    "bool": {
                        "filter": [
                            {"term": {"thread_id": thread_id}},
                            {"term": {"checkpoint_ns": checkpoint_ns}},
                            *self._identity_filters(tenant_id, user_id),
                        ]
                    }
                },
                sort=[{"checkpoint_id": {"order": "desc"}}],
                size=1,
            )
            hits = response["hits"]["hits"]
            if not hits:
                return None
            source = hits[0]["_source"]
        return await self._to_tuple(source, tenant_id, user_id)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        filters: list[dict] = []
        must_not: list[dict] = []
        tenant_id = user_id = None
        if config:
            configurable = config["configurable"]
            tenant_id, user_id = self._identity(config)
            filters.extend(self._identity_filters(tenant_id, user_id))
            if (thread_id := configurable.get("thread_id")) is not None:
                filters.append({"term": {"thread_id": thread_id}})
            if (checkpoint_ns := configurable.get("checkpoint_ns")) is not None:
                filters.append({"term": {"checkpoint_ns": checkpoint_ns}})
            if checkpoint_id := get_checkpoint_id(config):
                filters.append({"term": {"checkpoint_id": checkpoint_id}})
        if before and (before_id := get_checkpoint_id(before)):
            filters.append({"range": {"checkpoint_id": {"lt": before_id}}})
        for key, value in (filter or {}).items():
            field = f"metadata.{key}"
            if value is None:
                must_not.append({"exists": {"field": field}})
            else:
                filters.append({"term": {field: _term_value(value)}})

        if limit is not None and limit <= 0:
            return
        query = {"bool": {"filter": filters, "must_not": must_not}}
        sort = [
            {"checkpoint_id": {"order": "desc"}},
            {"thread_id": {"order": "asc"}},
            {"checkpoint_ns": {"order": "asc"}},
        ]
        remaining = limit
        search_after = None
        while True:
            size = _LIST_PAGE_SIZE if remaining is None else min(_LIST_PAGE_SIZE, remaining)
            kwargs: dict[str, Any] = {}
            if search_after is not None:
                kwargs["search_after"] = search_after
            response = await self.client.search(
                index=self.checkpoints_index, query=query, sort=sort, size=size, **kwargs
            )
            hits = response["hits"]["hits"]
            for hit in hits:
                yield await self._to_tuple(hit["_source"], tenant_id, user_id)
            if remaining is not None:
                remaining -= len(hits)
                if remaining <= 0:
                    return
            if len(hits) < size:
                return
            search_after = hits[-1]["sort"]

    async def _to_tuple(
        self, source: dict[str, Any], tenant_id: str | None, user_id: str | None
    ) -> CheckpointTuple:
        thread_id = source["thread_id"]
        checkpoint_ns = source["checkpoint_ns"]
        checkpoint = self.serde.loads_typed(
            (source["checkpoint_type"], base64.b64decode(source["checkpoint_data"]))
        )
        metadata = self.serde.loads_typed(
            (source["metadata_type"], base64.b64decode(source["metadata_data"]))
        )
        pending = await self._pending_writes(
            thread_id, checkpoint_ns, source["checkpoint_id"], tenant_id, user_id
        )
        identity = self._identity_config(tenant_id, user_id)
        parent_config = None
        if source.get("parent_checkpoint_id"):
            parent_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": source["parent_checkpoint_id"],
                    **identity,
                }
            }
        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": source["checkpoint_id"],
                    **identity,
                }
            },
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending,
        )

    async def _pending_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        tenant_id: str | None,
        user_id: str | None,
    ) -> list[tuple[str, str, Any]]:
        query = {
            "bool": {
                "filter": [
                    {"term": {"thread_id": thread_id}},
                    {"term": {"checkpoint_ns": checkpoint_ns}},
                    {"term": {"checkpoint_id": checkpoint_id}},
                    *self._identity_filters(tenant_id, user_id),
                ]
            }
        }
        # Same order as writes_sort_key: (task_path, task_id, idx).
        sort = [{"task_path": "asc"}, {"task_id": "asc"}, {"idx": "asc"}]
        result: list[tuple[str, str, Any]] = []
        search_after = None
        while True:
            kwargs: dict[str, Any] = {}
            if search_after is not None:
                kwargs["search_after"] = search_after
            response = await self.client.search(
                index=self.writes_index,
                query=query,
                sort=sort,
                size=_WRITES_PAGE_SIZE,
                **kwargs,
            )
            hits = response["hits"]["hits"]
            for hit in hits:
                s = hit["_source"]
                value = self.serde.loads_typed((s["value_type"], base64.b64decode(s["value"])))
                result.append((s["task_id"], s["channel"], value))
            if len(hits) < _WRITES_PAGE_SIZE:
                return result
            search_after = hits[-1]["sort"]

    async def adelete_thread(self, thread_id: str) -> None:
        for index in (self.checkpoints_index, self.writes_index):
            await self.client.delete_by_query(
                index=index,
                query={"term": {"thread_id": thread_id}},
                refresh=True,
                conflicts="proceed",
            )

    def get_next_version(self, current: str | None, channel: None) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        return f"{current_v + 1:032}.{random.random():016}"

    # Sync API is intentionally unsupported.

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        raise NotImplementedError(_ASYNC_ONLY)

    def list(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(_ASYNC_ONLY)

    def put(self, *args: Any, **kwargs: Any) -> RunnableConfig:
        raise NotImplementedError(_ASYNC_ONLY)

    def put_writes(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(_ASYNC_ONLY)

    def delete_thread(self, thread_id: str) -> None:
        raise NotImplementedError(_ASYNC_ONLY)
