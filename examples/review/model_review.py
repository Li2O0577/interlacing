"""Launch the three-question review or revise Q1 after all reviewers finish."""
import argparse
import os

from interlacing.collab.client import Client


def report(q2, q3):
    return {"forecast": q2, "baseline": q3}


GRAPH = {
    "nodes": [
        {"id": name, "kind": "human", "outputs": [{"name": "answer", "schema": "int"}]}
        for name in ("q1", "q2", "q3")
    ] + [{"id": "report", "kind": "function", "function": "model_review.report"}],
    "inputs": {"assumption": "q1.request", "baseline": "q3.request"},
    "edges": [{"from": "q1.answer", "to": "q2.request"},
              {"from": "q2.answer", "to": "report.q2"},
              {"from": "q3.answer", "to": "report.q3"}],
    "outputs": {"report": "report.out"},
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revise", metavar="COMPLETED_RUN")
    parser.add_argument("--value", type=int, default=12)
    parser.add_argument("--request-key", default="model-review-1")
    args = parser.parse_args()
    client = Client(os.getenv("INTERLACING_URL", "http://127.0.0.1:8765"), os.environ["MANAGER_TOKEN"])
    if args.revise:
        run = args.revise
        if client.request("status", run=run)["state"] != "done":
            parser.error("finish the original review before revising it")
        last = client.request("checkpoints", run=run)["last_event"]
        edit = {"node": "q1", "token": "answer#1", "version": 1, "value": args.value,
                "reason": "第一问采用了更新的样本，第二问需要按新假设重新计算。"}
        print(client.request("fork", run=run, at=last, output=edit, execute=True))
    else:
        print(client.request("start", graph=GRAPH, inputs={"assumption": "提交第一问的参数，例如 9",
                             "baseline": "独立计算第三问的基线，例如 5"},
                             assignments={"q1": "Alice", "q2": "Bob", "q3": "Alice"},
                             request_key=args.request_key))


if __name__ == "__main__":
    main()
