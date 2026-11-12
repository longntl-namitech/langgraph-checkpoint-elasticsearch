import os
import uuid

import pytest
from elasticsearch import AsyncElasticsearch
from langgraph.checkpoint.conformance import checkpointer_test, validate

from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver

pytestmark = pytest.mark.integration

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")


@checkpointer_test(name="AsyncElasticsearchSaver")
async def es_saver():
    client = AsyncElasticsearch(ES_URL)
    prefix = f"test_{uuid.uuid4().hex[:8]}_"
    cp_index, w_index = f"{prefix}checkpoints", f"{prefix}writes"
    try:
        saver = AsyncElasticsearchSaver(client, checkpoints_index=cp_index, writes_index=w_index)
        await saver.setup()
        yield saver
    finally:
        await client.indices.delete(index=[cp_index, w_index], ignore_unavailable=True)
        await client.close()


async def test_conformance(es_client):
    report = await validate(es_saver)
    report.print_report()
    assert report.passed_all_base()
