# Changelog

## 0.1.0

Public API:

- `AsyncElasticsearchSaver(client, *, checkpoints_index, writes_index, scoping=False, refresh="wait_for", serde=None)`: async LangGraph checkpoint saver. Methods: `setup`, `aput`, `aput_writes`, `aget_tuple`, `alist`, `adelete_thread`, `adelete_scoped_thread`. Sync methods raise `NotImplementedError`.
- `SessionOwnershipStore(client, *, index)`: `setup`, `claim_session`, `get_session_owner`.
- `ScopeError`: raised by a scoped saver when `tenant_id` or `user_id` is missing from `config["configurable"]`.

Behaviour notes:

- The `metadata` flattened field uses `ignore_above: 8191`, so a metadata or configurable string longer than Elasticsearch's 32766-byte term limit no longer fails `aput`.
- `adelete_thread` and `adelete_scoped_thread` retry once when `delete_by_query` reports version conflicts, then raise `RuntimeError` naming the index and the count left behind.
- `adelete_scoped_thread` raises `ValueError` for an empty or `None` `thread_id`, `tenant_id` or `user_id`, before any Elasticsearch call.
- `aput_writes` bulk failures raise `RuntimeError` listing each failed item's status and error type (first 5).
