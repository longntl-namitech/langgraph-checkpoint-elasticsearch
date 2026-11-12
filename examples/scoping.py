"""Two users on the same thread_id with scoping on; each sees only its own state.

Needs a running Elasticsearch (ES_URL, default http://localhost:9200). No LLM or API key.
"""

import asyncio
import operator
import os
from typing import Annotated, TypedDict

from elasticsearch import AsyncElasticsearch
from langgraph.graph import END, START, StateGraph

from langgraph_checkpoint_elasticsearch import AsyncElasticsearchSaver, ScopeError

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
CHECKPOINTS = "example_scoping_checkpoints"
WRITES = "example_scoping_writes"


class State(TypedDict):
    notes: Annotated[list[str], operator.add]


def noop(state: State) -> dict:
    return {}


def config_for(tenant_id: str, user_id: str) -> dict:
    return {"configurable": {"thread_id": "t1", "tenant_id": tenant_id, "user_id": user_id}}


async def main() -> None:
    client = AsyncElasticsearch(ES_URL)
    try:
        saver = AsyncElasticsearchSaver(
            client, checkpoints_index=CHECKPOINTS, writes_index=WRITES, scoping=True
        )
        await saver.setup()
        builder = StateGraph(State)
        builder.add_node("noop", noop)
        builder.add_edge(START, "noop")
        builder.add_edge("noop", END)
        graph = builder.compile(checkpointer=saver)

        alice = config_for("acme", "alice")
        bob = config_for("acme", "bob")
        await graph.ainvoke({"notes": ["alice's note"]}, alice)
        await graph.ainvoke({"notes": ["bob's note"]}, bob)

        print("alice sees:", (await graph.aget_state(alice)).values)
        print("bob sees:  ", (await graph.aget_state(bob)).values)

        try:
            await saver.aget_tuple({"configurable": {"thread_id": "t1"}})
        except ScopeError as exc:
            print("no identity ->", type(exc).__name__ + ":", exc)
    finally:
        await client.indices.delete(index=[CHECKPOINTS, WRITES], ignore_unavailable=True)
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
