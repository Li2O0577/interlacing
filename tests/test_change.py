"""A graph changed while it runs, through ctx.change only: checked, made at once, journaled, made again on resume."""
import pytest

from interlacing import Change, Chain, Engine, FunctionNode, GraphBuilder, Journal, Port, RunError
from interlacing.base import GraphError
from interlacing.spec import Catalog, load_node

HERE = __name__
FAILED = {"once": False}


def square(x):
    return x * x


def halve(x):
    return x / 2


def worker(id):
    return FunctionNode(id, square, inputs=[Port("x")], outputs=[Port("y")])


def plan(ctx, x):
    """Grows a branch for each number it is given, the first time it runs."""
    if ctx.round == 1:
        change = Change()
        for i in range(x):
            change.add(worker(f"w{i}")).connect("plan.y", f"w{i}.x").output(f"w{i}", f"w{i}.y")
        ctx.change(change)
    ctx.emit("y", x)


def planner():
    return FunctionNode("plan", plan, inputs=[Port("x", int)], outputs=[Port("y")], context=True)


@pytest.mark.parametrize("workers", [0, 4])
def test_a_node_grows_the_graph_it_runs_in(workers):
    seen = []
    engine = Engine(GraphBuilder().add(planner()).input("in", "plan.x").compile(), workers=workers, observe=seen.append)
    result = engine.run({"in": 3})
    assert {name: values for name, values in result.outputs.items()} == {"w0": [9], "w1": [9], "w2": [9]}
    assert result.rounds == {"plan": 1, "w0": 1, "w1": 1, "w2": 1}
    assert len(engine.graph.nodes) == 4 and engine.graph.digest != Engine(GraphBuilder().add(planner()).input("in", "plan.x").compile()).graph.digest
    changed = [e for e in seen if e["event"] == "changed"]
    assert len(changed) == 1 and changed[0]["added"] == ["w0", "w1", "w2"] and changed[0]["node"] == "plan"


def test_a_change_that_breaks_a_rule_is_refused_whole():
    told = []

    def bad(ctx, x):
        try:
            ctx.change(Change().add(worker("w")).connect("plan.y", "w.x").connect("w.y", "nowhere.x"))
        except GraphError as exc:
            told.append(str(exc))
        return x
    node = FunctionNode("plan", bad, inputs=[Port("x")], outputs=[Port("y")], context=True)
    engine = Engine(Chain(node).build())
    assert engine.run({"in": 1}).last("out") == 1
    assert "no node 'nowhere'" in told[0] and [n.id for n in engine.graph.nodes] == ["plan"]


def test_taking_nodes_out():
    seen = []

    def trim(ctx, x):
        if x == "trim":
            ctx.change(Change().remove("late"))
        else:
            with pytest.raises(GraphError, match="cannot take itself out"):
                ctx.change(Change().remove("first"))
        return x

    def hold(a, b):
        return [a, b]
    first = FunctionNode("first", trim, inputs=[Port("x")], outputs=[Port("y")], context=True)
    late = FunctionNode("late", hold, inputs=[Port("a"), Port("b")], outputs=[Port("y")])
    graph = (GraphBuilder().add(first, late).connect("first.y", "late.a").input("in", "first.x").input("other", "late.b")
             .output("out", "late.y").compile())
    result = Engine(graph, observe=seen.append).run({"in": "keep", "other": 1})
    assert result.last("out") == ["keep", 1]
    builder = GraphBuilder().add(FunctionNode("first", trim, inputs=[Port("x")], outputs=[Port("y")], context=True),
                                 FunctionNode("late", hold, inputs=[Port("a"), Port("b")], outputs=[Port("y")]))
    graph = builder.connect("first.y", "late.a").input("in", "first.x").input("other", "late.b").output("out", "late.y").compile()
    seen.clear()
    result = Engine(graph, observe=seen.append).run({"in": "trim", "other": 1})
    changed = next(e for e in seen if e["event"] == "changed")
    assert changed["removed"] == ["late"] and changed["dropped"] == 1      # what waited on late.b went with it
    assert result.outputs == {"out": []} and "late" not in result.rounds


def test_a_journaled_run_needs_a_way_to_make_added_nodes_again(tmp_path):
    graph = GraphBuilder().add(planner()).input("in", "plan.x").compile()
    with pytest.raises(RunError, match="rebuild"):
        Engine(graph, journal=Journal(tmp_path / "j.jsonl")).run({"in": 1})


def flaky(x):
    if not FAILED["once"]:
        FAILED["once"] = True
        raise RuntimeError("not yet")
    return x


def test_a_grown_graph_is_made_again_when_the_run_is_taken_up(tmp_path):
    FAILED["once"] = False

    def grow(ctx, x):
        ctx.change(Change().add(FunctionNode("sq", square, inputs=[Port("x")], outputs=[Port("y")]),
                                FunctionNode("flaky", flaky, inputs=[Port("x")], outputs=[Port("y")]))
                   .connect("grow.y", "sq.x").connect("sq.y", "flaky.x").output("result", "flaky.y"))
        return x
    graph = lambda: GraphBuilder().add(FunctionNode("grow", grow, inputs=[Port("x")], outputs=[Port("y")], context=True)) \
        .input("in", "grow.x").compile()  # noqa: E731
    rebuild = lambda described: load_node(described, Catalog(allow=[HERE]))  # noqa: E731
    path = tmp_path / "j.jsonl"
    with pytest.raises(RunError, match="not yet"):
        Engine(graph(), journal=Journal(path), rebuild=rebuild).run({"in": 3})
    engine = Engine(graph(), journal=Journal(path), rebuild=rebuild)
    result = engine.resume()
    assert result.outputs == {"result": [9]} and result.rounds == {"grow": 1, "sq": 1, "flaky": 1}
    assert {n.id for n in engine.graph.nodes} == {"grow", "sq", "flaky"}


def test_a_limit_on_how_large_the_graph_may_grow():
    graph = GraphBuilder().add(planner()).input("in", "plan.x").compile()
    with pytest.raises(RunError, match="at most 3 may run here"):
        Engine(graph, max_nodes=3).run({"in": 3})
    assert Engine(graph, max_nodes=4).run({"in": 3}).rounds["w2"] == 1


def test_a_change_must_be_a_change():
    def wrong(ctx, x):
        ctx.change({"add": "w"})
    graph = Chain(FunctionNode("n", wrong, inputs=[Port("x")], outputs=[Port("y")], context=True)).build()
    with pytest.raises(RunError, match="is a Change"):
        Engine(graph).run({"in": 1})
    assert halve(2) == 1


def grown(ctx, x):
    ctx.change(Change().add(FunctionNode("sq", square, inputs=[Port("x")], outputs=[Port("y")]),
                            FunctionNode("flaky", flaky, inputs=[Port("x")], outputs=[Port("y")]))
               .connect("grow.y", "sq.x").connect("sq.y", "flaky.x").output("result", "flaky.y"))
    return x


@pytest.mark.parametrize("backend", ["jsonl", "sqlite"])
def test_a_run_directory_takes_a_grown_graph_up(tmp_path, backend):
    from interlacing.runs import Run
    FAILED["once"] = False
    described = {"nodes": [{"id": "grow", "kind": "function", "function": f"{HERE}.grown", "context": True,
                            "inputs": ["x"], "outputs": ["y"]}], "inputs": {"in": "grow.x"}}
    run = Run.create(tmp_path / "run", described, allow=[HERE], journal=backend)
    assert run.start({"in": 4})["status"] == "failed"
    state = Run(tmp_path / "run").resume()
    assert state["status"] == "done" and state["outputs"] == {"result": [16]}
    assert set(run.show(timeline=True)["nodes"]) == {"grow", "sq", "flaky"}
