from langgraph_checkpoint_elasticsearch._mappings import (
    CHECKPOINTS_MAPPING,
    checkpoint_doc_id,
    write_doc_id,
)


def test_checkpoint_doc_id_unscoped():
    assert checkpoint_doc_id("t1", "", "c1") == "t1::c1"


def test_checkpoint_doc_id_scoped():
    assert checkpoint_doc_id("t1", "", "c1", "acme", "u1") == "acme:u1:t1::c1"


def test_doc_id_parts_are_escaped():
    assert checkpoint_doc_id("a:b", "", "c") != checkpoint_doc_id("a", "b", "c")


def test_write_doc_id_includes_task_and_idx():
    assert write_doc_id("t1", "", "c1", "task", -1) == "t1::c1:task:-1"


def test_mappings_store_payloads_unindexed():
    props = CHECKPOINTS_MAPPING["mappings"]["properties"]
    assert props["checkpoint_data"]["type"] == "binary"
    assert props["metadata"]["type"] == "flattened"
