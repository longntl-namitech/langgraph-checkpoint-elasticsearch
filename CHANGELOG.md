# Changelog

## 0.1.0

Public API:

- `AsyncElasticsearchSaver(client, *, checkpoints_index, writes_index, scoping=False, refresh="wait_for", serde=None)`: async LangGraph checkpoint saver. Methods: `setup`, `aput`, `aput_writes`, `aget_tuple`, `alist`, `adelete_thread`, `adelete_scoped_thread`. Sync methods raise `NotImplementedError`.
- `SessionOwnershipStore(client, *, index)`: `setup`, `claim_session`, `get_session_owner`.
- `ScopeError`: raised by a scoped saver when `tenant_id` or `user_id` is missing from `config["configurable"]`.
