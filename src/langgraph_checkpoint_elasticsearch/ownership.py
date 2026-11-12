from __future__ import annotations

from datetime import UTC, datetime

from elasticsearch import AsyncElasticsearch, BadRequestError, ConflictError, NotFoundError

from langgraph_checkpoint_elasticsearch._mappings import OWNERS_MAPPING


class SessionOwnershipStore:
    """Records which tenant/user owns a session (thread)."""

    def __init__(
        self, client: AsyncElasticsearch, *, index: str = "langgraph_session_owners"
    ) -> None:
        self.client = client
        self.index = index

    async def setup(self) -> None:
        """Create the owners index if it is missing."""
        if await self.client.indices.exists(index=self.index):
            return
        try:
            await self.client.indices.create(index=self.index, mappings=OWNERS_MAPPING["mappings"])
        except BadRequestError as exc:
            # A concurrent setup() may have created it first.
            if exc.error != "resource_already_exists_exception":
                raise

    async def get_session_owner(self, thread_id: str) -> tuple[str, str] | None:
        """Return (tenant_id, user_id) of the session owner, or None if unclaimed."""
        try:
            doc = await self.client.get(index=self.index, id=thread_id)
        except NotFoundError:
            return None
        source = doc["_source"]
        return source["tenant_id"], source["user_id"]

    async def claim_session(self, thread_id: str, tenant_id: str, user_id: str) -> bool:
        """Claim a session. True if created or already owned by this tenant/user."""
        if not thread_id or not tenant_id or not user_id:
            raise ValueError("thread_id, tenant_id and user_id must be non-empty")
        try:
            await self.client.index(
                index=self.index,
                id=thread_id,
                op_type="create",
                document={
                    "thread_id": thread_id,
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "claimed_at": datetime.now(UTC).isoformat(),
                },
                refresh="wait_for",
            )
            return True
        except ConflictError:
            owner = await self.get_session_owner(thread_id)
            return owner == (tenant_id, user_id)
