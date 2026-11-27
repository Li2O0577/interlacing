"""Optional comparison probes. See docs/LANGGRAPH_COMPARISON.md for pinned dependencies."""
import argparse
from collections import Counter
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic_ns, sleep
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
    observed = {}
    start = monotonic_ns()

    def slow(state):
        sleep(0.05)
        observed["slow_done"] = monotonic_ns()
        return {"slow_value": 3}

    def consume(state):
        observed["consume"] = monotonic_ns()
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
        for update in graph.stream({}, config, stream_mode="updates", durability="sync"):
            if "fast" in update:
                observed["observer"] = monotonic_ns()
        assert graph.get_state(config).values["received"] == 2
    assert observed["slow_done"] <= observed["consume"]
    print(" ".join(f"{name}={(observed[name] - start) / 1e6:.2f}ms"
                   for name in ("observer", "slow_done", "consume")))
    return "observer_before_slow" if observed["observer"] < observed["slow_done"] else "observer_at_or_after_slow"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=10)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be positive")
    print("langgraph", version("langgraph"), "langgraph-checkpoint-sqlite", version("langgraph-checkpoint-sqlite"))
    with TemporaryDirectory() as temp:
        check_review(Path(temp) / "review.sqlite")
        counts = Counter(check_scheduling(Path(temp) / f"branches-{i}.sqlite") for i in range(args.repeat))
        print("Observer before slow completion:", counts["observer_before_slow"],
              "at or after:", counts["observer_at_or_after_slow"])
