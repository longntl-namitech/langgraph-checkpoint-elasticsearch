"""Run a graph, then resume it from Elasticsearch with a new client and saver.

Needs a running Elasticsearch (ES_URL, default http://localhost:9200). No LLM or API key.
"""

import asyncio
import operator
import os
from typing import Annotated, TypedDict

from elasticsearch import AsyncElasticsearch
from langgraph.graph import END, START, StateGraph

from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
CHECKPOINTS = "example_resume_checkpoints"
WRITES = "example_resume_writes"


class State(TypedDict):
    count: int
    log: Annotated[list[str], operator.add]


def increment(state: State) -> dict:
    return {"count": state["count"] + 1, "log": [f"count -> {state['count'] + 1}"]}


async def run(config: dict, payload: dict) -> State:
    client = AsyncElasticsearch(ES_URL)
    try:
        saver = AsyncElasticsearchSaver(client, checkpoints_index=CHECKPOINTS, writes_index=WRITES)
        await saver.setup()
        builder = StateGraph(State)
        builder.add_node("increment", increment)
        builder.add_edge(START, "increment")
        builder.add_edge("increment", END)
        graph = builder.compile(checkpointer=saver)
        return await graph.ainvoke(payload, config)
    finally:
        await client.close()


async def main() -> None:
    config = {"configurable": {"thread_id": "demo"}}
    cleanup = AsyncElasticsearch(ES_URL)
    try:
        first = await run(config, {"count": 0, "log": []})
        print("first run:", first)
        # A new client and saver simulate a restart. "count" is not passed, so it
        # continues from the value saved in Elasticsearch.
        second = await run(config, {"log": ["restarted"]})
        print("after restart:", second)
    finally:
        await cleanup.indices.delete(index=[CHECKPOINTS, WRITES], ignore_unavailable=True)
        await cleanup.close()


if __name__ == "__main__":
    asyncio.run(main())
