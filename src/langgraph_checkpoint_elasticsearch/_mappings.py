from urllib.parse import quote

_KEYWORD = {"type": "keyword"}

CHECKPOINTS_MAPPING: dict = {
    "mappings": {
        "properties": {
            "tenant_id": _KEYWORD,
            "user_id": _KEYWORD,
            "thread_id": _KEYWORD,
            "checkpoint_ns": _KEYWORD,
            "checkpoint_id": _KEYWORD,
            "parent_checkpoint_id": _KEYWORD,
            "checkpoint_type": _KEYWORD,
            "metadata_type": _KEYWORD,
            "checkpoint_data": {"type": "binary"},
            "metadata_data": {"type": "binary"},
            "metadata": {"type": "flattened", "ignore_above": 8191},
            "created_at": {"type": "date"},
        }
    }
}

WRITES_MAPPING: dict = {
    "mappings": {
        "properties": {
            "tenant_id": _KEYWORD,
            "user_id": _KEYWORD,
            "thread_id": _KEYWORD,
            "checkpoint_ns": _KEYWORD,
            "checkpoint_id": _KEYWORD,
            "task_id": _KEYWORD,
            "task_path": _KEYWORD,
            "channel": _KEYWORD,
            "value_type": _KEYWORD,
            "idx": {"type": "integer"},
            "value": {"type": "binary"},
        }
    }
}

OWNERS_MAPPING: dict = {
    "mappings": {
        "properties": {
            "thread_id": _KEYWORD,
            "tenant_id": _KEYWORD,
            "user_id": _KEYWORD,
            "claimed_at": {"type": "date"},
        }
    }
}


def _join(parts: list[str], tenant_id: str | None, user_id: str | None) -> str:
    encoded = [quote(p, safe="") for p in parts]
    if tenant_id is not None and user_id is not None:
        encoded = [quote(tenant_id, safe=""), quote(user_id, safe=""), *encoded]
    return ":".join(encoded)


def checkpoint_doc_id(
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    tenant_id: str | None = None,
    user_id: str | None = None,
) -> str:
    return _join([thread_id, checkpoint_ns, checkpoint_id], tenant_id, user_id)


def write_doc_id(
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    task_id: str,
    idx: int,
    tenant_id: str | None = None,
    user_id: str | None = None,
) -> str:
    return _join([thread_id, checkpoint_ns, checkpoint_id, task_id, str(idx)], tenant_id, user_id)
