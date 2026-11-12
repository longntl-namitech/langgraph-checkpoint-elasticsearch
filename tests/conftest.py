import os
import uuid

import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError


@pytest_asyncio.fixture
async def es_client():
    client = AsyncElasticsearch(os.environ.get("ES_URL", "http://localhost:9200"))
    try:
        if not await client.ping():
            await client.close()
            pytest.skip("Elasticsearch is not reachable")
    except ESConnectionError:
        await client.close()
        pytest.skip("Elasticsearch is not reachable")
    except BaseException:
        await client.close()
        raise
    try:
        yield client
    finally:
        await client.close()


@pytest_asyncio.fixture
async def index_prefix(es_client):
    prefix = f"test_{uuid.uuid4().hex[:8]}_"
    yield prefix
    found = await es_client.indices.get(index=f"{prefix}*", ignore_unavailable=True)
    if found:
        await es_client.indices.delete(index=list(found), ignore_unavailable=True)
