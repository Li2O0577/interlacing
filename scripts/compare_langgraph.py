"""Optional comparison probes. See docs/LANGGRAPH_COMPARISON.md for pinned dependencies."""
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from typing import TypedDict

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, RetryPolicy, interrupt


class Quote(TypedDict, total=False):
    price: int
    approved: bool


def check_review(path):
    attempts = []
    reviews = []

    def price(state):
        attempts.append(state["price"])
        if len(attempts) == 1:
            raise ConnectionError("simulated temporary failure")
        return {"price": state["price"] + 1}

    def review(state):
        reviews.append(state["price"])
        return {"approved": interrupt({"quote": state["price"]})}

    builder = StateGraph(Quote)
    builder.add_node("price_quote", price, retry_policy=RetryPolicy(
        max_attempts=2, retry_on=ConnectionError, initial_interval=0.001, jitter=False))
    builder.add_node("review", review)
    builder.add_edge(START, "price_quote")
    builder.add_edge("price_quote", "review")
    builder.add_edge("review", END)
    config = {"configurable": {"thread_id": "review"}}
    with SqliteSaver.from_conn_string(str(path)) as saver:
        graph = builder.compile(checkpointer=saver)
        graph.invoke({"price": 9}, config)
        pending = graph.get_state(config)
        assert pending.next == ("review",)
        assert pending.tasks[0].interrupts[0].value == {"quote": 10}

    with SqliteSaver.from_conn_string(str(path)) as saver:
        graph = builder.compile(checkpointer=saver)
        result = graph.invoke(Command(resume=True), config)
        assert result["price"] == 10 and result["approved"] is True
        assert len(list(graph.get_state_history(config))) >= 3
    assert attempts == [9, 9] and reviews == [10, 10]
    print("LangGraph: retry succeeded, SQLite reopened, review node re-entered, approval restored.")


class Branches(TypedDict, total=False):
    fast_value: int
    slow_value: int
    received: int


def check_scheduling(path):
    release_slow = Event()
    slow_done = Event()
    received = Event()

    def slow(state):
        if not release_slow.wait(10):
            raise TimeoutError("fast update never reached the observer")
        slow_done.set()
        return {"slow_value": 3}

    def consume(state):
        assert slow_done.is_set()
        received.set()
        return {"received": state["fast_value"]}

    builder = StateGraph(Branches)
    builder.add_node("fast", lambda state: {"fast_value": 2})
    builder.add_node("slow", slow)
    builder.add_node("consume", consume)
    builder.add_edge(START, "fast")
    builder.add_edge(START, "slow")
    builder.add_edge("fast", "consume")
    builder.add_edge("consume", END)
    builder.add_edge("slow", END)
    config = {"configurable": {"thread_id": "branches"}}
    with SqliteSaver.from_conn_string(str(path)) as saver:
        graph = builder.compile(checkpointer=saver)
        try:
            for update in graph.stream({}, config, stream_mode="updates", durability="sync"):
                if "fast" in update:
                    assert not slow_done.is_set() and not received.is_set()
                    release_slow.set()
        finally:
            release_slow.set()
        assert received.is_set() and graph.get_state(config).values["received"] == 2
    print("LangGraph: observer saw fast update before slow finished; dependent node ran after slow finished.")


if __name__ == "__main__":
    print("langgraph", version("langgraph"), "langgraph-checkpoint-sqlite", version("langgraph-checkpoint-sqlite"))
    with TemporaryDirectory() as temp:
        check_review(Path(temp) / "review.sqlite")
        check_scheduling(Path(temp) / "branches.sqlite")
