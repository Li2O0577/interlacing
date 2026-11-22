"""The switch on each node: pause, resume, stop and restart, one node or several, by the operator or a manager."""
import asyncio
import json
import os
import threading
import time

import pytest

from interlacing import Engine, FunctionNode, GraphBuilder, Journal, Node, Port, RunError
from interlacing.base import GraphError, NodeError
from interlacing.cli import main
from interlacing.runs import Run

HERE = __name__


def stepper(name, steps, seen, pause=0.01):
    def run(ctx, x):
        for i in range(steps):
            if not ctx.checkpoint():
                return None
            seen.append((name, i))
            time.sleep(pause)
        return x
    return FunctionNode(name, run, inputs=[Port('x')], outputs=[Port('y')], context=True)


def side_by_side(*nodes):
    builder = GraphBuilder().add(*nodes)
    for node in nodes:
        builder.input(node.id, f'{node.id}.x').output(node.id, f'{node.id}.y')
    return builder


def in_background(engine, inputs):
    box = {}

    def go():
        try:
            box['result'] = engine.run(inputs)
        except BaseException as exc:      # noqa: BLE001
            box['error'] = exc
    thread = threading.Thread(target=go)
    thread.start()
    return thread, box


def until(condition, limit=5.0):
    end = time.monotonic() + limit
    while not condition():
        assert time.monotonic() < end, 'waited too long'
        time.sleep(0.005)


def test_one_node_is_paused_while_the_rest_goes_on_and_then_resumed():
    seen = []
    graph = side_by_side(stepper('a', 30, seen), stepper('b', 100, seen)).compile()
    engine = Engine(graph, workers=2)
    thread, box = in_background(engine, {'a': 1, 'b': 2})
    until(lambda: ('a', 2) in seen)
    assert engine.control('a', 'pause') == ['a']
    time.sleep(0.05)
    held, went = sum(1 for name, _ in seen if name == 'a'), sum(1 for name, _ in seen if name == 'b')
    time.sleep(0.15)
    assert sum(1 for name, _ in seen if name == 'a') == held     # a waits at its step
    assert sum(1 for name, _ in seen if name == 'b') > went      # b goes on
    assert engine.control('a', 'resume') == ['a']
    thread.join(10)
    result = box['result']
    assert result.outputs == {'a': [1], 'b': [2]} and result.complete and not result.held
    assert [i for name, i in seen if name == 'a'] == list(range(30))    # it went on from where it waited


def test_several_at_once_and_a_switch_that_is_already_set():
    seen = []
    graph = side_by_side(stepper('a', 40, seen), stepper('b', 40, seen), stepper('c', 5, seen)).compile()
    engine = Engine(graph, workers=3)
    thread, box = in_background(engine, {'a': 1, 'b': 2, 'c': 3})
    until(lambda: ('a', 1) in seen and ('b', 1) in seen)
    assert engine.control(['a', 'b'], 'pause') == ['a', 'b']
    assert engine.control(['a', 'b'], 'pause') == []          # nothing changed
    until(lambda: ('c', 4) in seen)
    engine.control(['a', 'b'], 'resume')
    thread.join(10)
    assert box['result'].outputs == {'a': [1], 'b': [2], 'c': [3]}
    with pytest.raises(RunError, match='only a run that is going'):
        engine.control('a', 'pause')
    with pytest.raises(ValueError, match='no action'):
        Engine(graph).control('a', 'nap')


def test_a_stopped_node_gives_its_work_back_and_the_run_is_left_to_be_taken_up(tmp_path):
    seen = []
    build = lambda: side_by_side(stepper('a', 50, seen), stepper('b', 3, seen)).compile()   # noqa: E731
    path = tmp_path / 'run.jsonl'
    engine = Engine(build(), workers=2, journal=Journal(path))
    thread, box = in_background(engine, {'a': 1, 'b': 2})
    until(lambda: ('a', 2) in seen)
    engine.control('a', 'stop')
    thread.join(10)
    result = box['result']
    assert result.outputs == {'a': [], 'b': [2]} and not result.complete and result.held == {'a': 'stopped'}
    # still stopped when taken up: nothing happens until it is switched on again
    again = Engine(build(), journal=Journal(path)).resume()
    assert again.outputs == {'a': [], 'b': [2]} and again.held == {'a': 'stopped'}
    Journal(path).write({'kind': 'control', 'nodes': ['a'], 'action': 'resume'})
    seen.clear()
    last = Engine(build(), journal=Journal(path)).resume()
    assert last.outputs == {'a': [1], 'b': [2]} and last.complete and [i for _, i in seen] == list(range(50))


class Counter(Node):
    """Holds how many items it has done, across activations; a restart forgets it."""
    kind = 'counter_for_test'

    def __init__(self, id, gate):
        super().__init__(id, inputs=[Port('x')], outputs=[Port('y')])
        self.done, self.resets, self.gate = 0, 0, gate

    def reset(self):
        self.done, self.resets = 0, self.resets + 1

    def run(self, ctx, inputs):
        for _ in range(3):
            self.gate.wait(5)
            if not ctx.checkpoint():
                return None
            self.done += 1
        return {'y': self.done}


def test_a_restart_drops_what_the_node_was_doing_and_what_it_held():
    gate = threading.Event()
    node = Counter('n', gate)
    engine = Engine(GraphBuilder().add(node).input('in', 'n.x').output('out', 'n.y').compile(), workers=1)
    thread, box = in_background(engine, {'in': 1})
    until(lambda: engine._states['n'].running)
    node.done = 7                                 # as if it had done work before
    engine.control('n', 'restart')
    gate.set()
    thread.join(10)
    assert box['result'].last('out') == 3 and node.resets == 1 and box['result'].rounds == {'n': 2}


def manager(name, act):
    def run(ctx, x):
        return act(ctx, x)
    return FunctionNode(name, run, inputs=[Port('x')], outputs=[Port('y')], context=True)


def test_a_manager_controls_only_those_under_it():
    seen, told = [], []

    def act(ctx, x):
        until(lambda: ('b', 1) in seen and ('c', 1) in seen)
        told.append(ctx.control('b', 'pause', team=True))       # b and c, which b manages
        try:
            ctx.control('other', 'pause')
        except NodeError as exc:
            told.append(str(exc))
        time.sleep(0.1)
        told.append(ctx.control('c', 'resume'))
        return x
    graph = (side_by_side(manager('boss', act), stepper('b', 30, seen), stepper('c', 20, seen), stepper('other', 3, seen))
             .manage('boss', 'b').manage('b', 'c').compile())
    assert graph.team('boss') == ('b', 'c') and graph.team('boss', deep=False) == ('b',)
    assert graph.describe()['manages'] == {'b': ['c'], 'boss': ['b']}
    result = Engine(graph, workers=4).run({'boss': 0, 'b': 1, 'c': 2, 'other': 3})
    assert told[0] == ['b', 'c'] and 'boss does not manage other' in told[1] and told[2] == ['c']
    assert result.outputs['c'] == [2] and result.outputs['b'] == [] and result.held == {'b': 'paused'}
    assert not result.complete


def test_who_manages_whom_is_a_tree():
    def nodes():
        return [FunctionNode(n, lambda x: x, inputs=[Port('x')], outputs=[Port('y')]) for n in ('a', 'b', 'c')]

    def build():
        builder = GraphBuilder().add(*nodes())
        for n in 'abc':
            builder.input(n, f'{n}.x')
        return builder
    with pytest.raises(GraphError, match='manage itself'):
        build().manage('a', 'b').manage('b', 'a').compile()
    with pytest.raises(GraphError, match='manage itself'):
        build().manage('a', 'a').compile()
    with pytest.raises(GraphError, match='already managed by a'):
        build().manage('a', 'c').manage('b', 'c')
    with pytest.raises(GraphError, match="no node 'x'"):
        build().manage('x', 'a').compile()
    plain = build().compile()
    assert 'manages' not in plain.describe()             # graphs without it keep their digest
    graph = build().manage('a', 'b').compile()
    from interlacing.base import Change
    assert GraphBuilder.of(graph).compile().team('a') == ('b',)
    assert Change().remove('b').apply(GraphBuilder.of(graph)).compile().team('a') == ()


def test_a_paused_coroutine_waits_without_holding_the_loop():
    seen = []

    async def steps(ctx, x):
        for i in range(20):
            if not await ctx.acheckpoint():
                return None
            seen.append(i)
            await asyncio.sleep(0.01)
        return x
    async def meanwhile(x):
        await asyncio.sleep(0.4)
        return x
    node = FunctionNode('a', steps, inputs=[Port('x')], outputs=[Port('y')], context=True)
    other = FunctionNode('b', meanwhile, inputs=[Port('x')], outputs=[Port('y')])
    engine = Engine(side_by_side(node, other).compile(), workers=2)

    async def main():
        task = asyncio.create_task(engine.arun({'a': 5, 'b': 6}))
        while len(seen) < 3:
            await asyncio.sleep(0.005)
        engine.control('a', 'pause')
        held = len(seen)
        await asyncio.sleep(0.1)
        assert len(seen) <= held + 1
        engine.control('a', 'resume')
        return await task
    assert asyncio.run(main()).outputs == {'a': [5], 'b': [6]} and seen == list(range(20))


def test_time_spent_paused_does_not_count_against_the_limit():
    seen = []
    long = stepper('b', 150, seen)
    long.time_limit = 10
    engine = Engine(side_by_side(stepper('a', 10, seen), long).compile(), time_limit=0.6, workers=2)
    thread, box = in_background(engine, {'a': 1, 'b': 2})
    until(lambda: ('a', 1) in seen)
    engine.control('a', 'pause')
    time.sleep(0.8)
    engine.control('a', 'resume')
    thread.join(10)
    assert 'result' in box, box.get('error')
    assert box['result'].outputs == {'a': [1], 'b': [2]}


def test_when_all_that_is_left_is_paused_the_run_ends_to_be_taken_up(tmp_path):
    seen = []
    build = lambda: GraphBuilder().add(stepper('a', 20, seen)).input('in', 'a.x').output('out', 'a.y').compile()   # noqa: E731
    path = tmp_path / 'run.jsonl'
    engine = Engine(build(), journal=Journal(path), workers=1)
    thread, box = in_background(engine, {'in': 4})
    until(lambda: ('a', 3) in seen)
    engine.control('a', 'pause')
    thread.join(10)                                 # nothing else could happen: it did not wait for ever
    assert box['result'].held == {'a': 'paused'} and not box['result'].complete
    Journal(path).write({'kind': 'control', 'nodes': ['a'], 'action': 'resume'})
    assert Engine(build(), journal=Journal(path)).resume().last('out') == 4


def test_a_paused_node_is_not_called():
    def asks(ctx, x):
        ctx.control('helper', 'pause')
        try:
            return ctx.call('ask', x)
        except NodeError as exc:
            return str(exc)
    caller = FunctionNode('caller', asks, inputs=[Port('x')], outputs=[Port('y'), Port('ask', call=True)], context=True)
    helper = FunctionNode('helper', lambda x: x, inputs=[Port('x')], outputs=[Port('y')])
    graph = (GraphBuilder().add(caller, helper).connect('caller.ask', 'helper.x', 'call').manage('caller', 'helper')
             .input('in', 'caller.x').output('out', 'caller.y').compile())
    assert 'helper is paused' in Engine(graph).run({'in': 1}).last('out')


def stopping_boss(ctx, x):
    ctx.control('worker', 'stop')
    return x


def plain(ctx, x):
    for _ in range(3):
        if not ctx.checkpoint():
            return None
    return x * 10


def test_a_run_directory_is_controlled_between_goes(tmp_path, capsys):
    described = {"nodes": [{"id": "boss", "kind": "function", "function": f"{HERE}.stopping_boss", "context": True,
                            "inputs": ["x"], "outputs": ["y"]},
                           {"id": "worker", "kind": "function", "function": f"{HERE}.plain", "context": True,
                            "inputs": ["x"], "outputs": ["y"]}],
                 "edges": [{"from": "boss.y", "to": "worker.x"}],
                 "inputs": {"in": "boss.x"}, "outputs": {"out": "worker.y"}, "manages": {"boss": ["worker"]}}
    run = Run.create(tmp_path / 'run', described, allow=[HERE])
    state = run.start({'in': 2})
    assert state['status'] == 'stopped' and state['held'] == {'worker': 'stopped'}
    assert Run(tmp_path / 'run').status()['switches'] == {'worker': 'stopped'}
    code = main(['control', 'resume', 'worker', '--dir', str(tmp_path / 'run')])
    out = json.loads(capsys.readouterr().out)
    assert code == 0 and out['changed'] == ['worker'] and 'switches' not in out
    assert main(['control', 'pause', 'nobody', '--dir', str(tmp_path / 'run')]) == 1
    assert 'no node nobody' in capsys.readouterr().out
    state = Run(tmp_path / 'run').resume()
    assert state['status'] == 'done' and state['outputs'] == {'out': [20]}


def test_an_agent_forgets_its_conversation_when_restarted():
    from interlacing import AgentNode
    from interlacing.models import FakeProvider
    agent = AgentNode('a', FakeProvider(lambda system, messages, tools: None))
    agent.messages = [{'role': 'user', 'content': 'earlier'}]
    agent.reset()
    assert agent.messages == []


def test_a_restart_the_node_never_looked_at_still_forgets_what_it_held():
    gate, started = threading.Event(), threading.Event()
    resets = []

    class Deaf(Node):
        kind = 'deaf'
        def __init__(self):
            super().__init__('d', inputs=[Port('x')], outputs=[Port('y')])

        def run(self, ctx, inputs):
            started.set()
            gate.wait(5)                          # it never asks whether it should stop
            return {'y': inputs['x']}

        def reset(self):
            resets.append(1)

    engine = Engine(GraphBuilder().add(Deaf()).input('in', 'd.x').output('out', 'd.y').compile(), workers=1)
    thread, box = in_background(engine, {'in': 4})
    assert started.wait(5)
    engine.control('d', 'restart')
    gate.set()
    thread.join(10)
    assert box['result'].last('out') == 4 and resets == [1] and engine._states['d'].halt is False


def test_a_node_answering_a_call_finishes_the_answer_when_it_is_stopped():
    seen = []

    def caller(ctx, x):
        return ctx.call('ask', x)
    node = FunctionNode('caller', caller, context=True, inputs=[Port('x')], outputs=[Port('y'), Port('ask', call=True)])
    graph = (GraphBuilder().add(node, stepper('callee', 30, seen)).connect('caller.ask', 'callee.x', 'call')
             .input('in', 'caller.x').output('out', 'caller.y').compile())
    engine = Engine(graph, workers=2)
    thread, box = in_background(engine, {'in': 1})
    until(lambda: len(seen) > 2)
    assert engine.control('callee', 'stop') == ['callee']
    thread.join(10)
    result = box['result']
    assert result.last('out') == 1 and len(seen) == 30 and result.held == {'callee': 'stopped'}


def test_a_served_node_is_paused_at_its_own_steps():
    from interlacing import RemoteNode, serve_node
    seen = []
    with serve_node(stepper('far', 40, seen, 0.01)) as server:
        graph = side_by_side(RemoteNode('far', server.address), stepper('other', 400, [], 0.01)).compile()
        engine = Engine(graph, workers=2)
        thread, box = in_background(engine, {'far': 5, 'other': 1})
        until(lambda: len(seen) > 3)
        engine.control('far', 'pause')
        time.sleep(0.4)
        held = len(seen)
        time.sleep(0.3)
        assert len(seen) == held < 40                  # the far side waits at its step
        engine.control('far', 'resume')
        until(lambda: len(seen) == 40)
        engine.control('other', 'stop')
        thread.join(10)
    assert box['result'].last('far') == 5 and len(seen) == 40


def test_a_served_node_that_is_left_paused_ends_the_run_and_is_told_to_stop():
    from interlacing import RemoteNode, serve_node
    seen = []
    with serve_node(stepper('far', 40, seen, 0.01)) as server:
        graph = GraphBuilder().add(RemoteNode('far', server.address)).input('in', 'far.x').output('out', 'far.y').compile()
        engine = Engine(graph, workers=1)
        thread, box = in_background(engine, {'in': 5})
        until(lambda: len(seen) > 3)
        engine.control('far', 'pause')
        thread.join(10)
        result = box['result']
        assert not result.complete and result.held == {'far': 'paused'} and not result.outputs.get('out')
        time.sleep(0.3)
        assert len(seen) < 40                          # stopped there, not run to its end


def test_a_program_is_paused_with_its_node_and_its_time_limit_waits(tmp_path):
    import sys
    from interlacing.nodes import ProcessNode
    ticks = tmp_path / 'ticks'
    script = (f"import time\nfor _ in range(20):\n    open({str(ticks)!r}, 'a').write('x')\n    time.sleep(0.05)\n"
              "print('done')")
    program = ProcessNode('p', [sys.executable, '-c', script], timeout=3, takes_input=False)
    other = stepper('other', 400, [], 0.01)
    builder = GraphBuilder().add(program, other).input('o', 'other.x').output('out', 'p.out').output('o', 'other.y')
    engine = Engine(builder.compile(), workers=2)
    thread, box = in_background(engine, {'o': 1})
    until(lambda: ticks.exists() and ticks.stat().st_size > 2)
    engine.control('p', 'pause')
    time.sleep(0.3)
    held = ticks.stat().st_size
    time.sleep(3)                                  # longer than the program's own time limit
    if os.name == 'posix':
        assert ticks.stat().st_size == held < 20   # the program itself is held
    engine.control('p', 'resume')
    engine.control('other', 'stop')
    thread.join(20)
    assert [o.strip() for o in box['result'].outputs['out']] == ['done'] and ticks.stat().st_size == 20


def test_another_process_controls_a_run_that_is_going(tmp_path):
    described = {"nodes": [{"id": "slow", "kind": "function", "function": f"{HERE}.slow_steps", "context": True,
                            "inputs": ["x"], "outputs": ["y"]}],
                 "inputs": {"in": "slow.x"}, "outputs": {"out": "slow.y"}}
    Run.create(tmp_path / 'run', described, allow=[HERE])
    box = {}
    thread = threading.Thread(target=lambda: box.update(state=Run(tmp_path / 'run').start({'in': 3})))
    thread.start()
    until(lambda: SLOW and Run(tmp_path / 'run').status()['status'] == 'running')
    said = Run(tmp_path / 'run').control('slow', 'stop')       # not the object running it: as from another process
    assert said['sent'] and said['changed'] == ['slow'] and said['switches'] == {'slow': 'stopped'}
    thread.join(10)
    assert box['state']['status'] == 'stopped' and box['state']['held'] == {'slow': 'stopped'}
    assert len(SLOW) < 200 and not list((tmp_path / 'run' / 'controls').glob('*.json'))
    assert Run(tmp_path / 'run').control('slow', 'stop')['changed'] == []      # already stopped: nothing changes
    assert Run(tmp_path / 'run').control('slow', 'resume')['changed'] == ['slow']
    SLOW.clear()
    state = Run(tmp_path / 'run').resume()
    assert state['status'] == 'done' and state['outputs'] == {'out': [3]}


SLOW = []


def slow_steps(ctx, x):
    for _ in range(200):
        if not ctx.checkpoint():
            return None
        SLOW.append(1)
        time.sleep(0.01)
    return x


def test_an_agent_paused_while_its_model_answers_holds_the_answer_until_resumed():
    from interlacing import AgentNode
    from interlacing.models import ChatResult, FakeProvider
    asked, release = threading.Event(), threading.Event()

    def slow(system, messages, tools):
        asked.set()
        release.wait(5)
        return ChatResult(text='the answer')
    agent = AgentNode('a', FakeProvider(slow))
    seen = []
    graph = (GraphBuilder().add(agent, stepper('b', 200, seen)).input('task', 'a.task').output('out', 'a.out')
             .input('b', 'b.x').output('b', 'b.y').compile())   # b keeps the run going while a is paused
    sent = []
    engine = Engine(graph, workers=2,
                    observe=lambda event: sent.append(event) if event.get('event') == 'emitted' and event.get('node') == 'a' else None)
    thread, box = in_background(engine, {'task': 'go', 'b': 1})
    until(asked.is_set)
    engine.control('a', 'pause')
    release.set()                    # the model answers while the agent is paused
    time.sleep(0.2)
    assert not sent                  # the answer is held at the step
    engine.control('a', 'resume')
    thread.join(10)
    assert box['result'].outputs['out'] == ['the answer'] and not box['result'].held


def test_an_agent_stopped_while_its_model_answers_does_not_send_the_answer(tmp_path):
    from interlacing import AgentNode
    from interlacing.models import ChatResult, FakeProvider
    asked, release = threading.Event(), threading.Event()

    def slow(system, messages, tools):
        asked.set()
        release.wait(5)
        return ChatResult(text='the answer')
    agent = AgentNode('a', FakeProvider(slow))
    graph = GraphBuilder().add(agent).input('task', 'a.task').output('out', 'a.out').compile()
    engine = Engine(graph, workers=2, journal=Journal(tmp_path / 'j.jsonl'))
    thread, box = in_background(engine, {'task': 'go'})
    until(asked.is_set)
    engine.control('a', 'stop')
    release.set()
    thread.join(10)
    assert box['result'].outputs == {'out': []} and box['result'].held == {'a': 'stopped'}
