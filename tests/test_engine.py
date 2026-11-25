"""The engine runs compiled graphs: chains, acyclic graphs, fan-out and fan-in, loops with a round limit."""
import threading
import time

import pytest

from interlacing.base import Firing, GraphBuilder, GraphError, Port
from interlacing.engine import Engine, RunError
from interlacing.methods import Chain
from interlacing.nodes import FunctionNode
from interlacing.protocols import DEFAULT

WORKERS = [0, 1, 4]


def fn(id, function, inputs=('x',), outputs=('y',), **options):
    ports = lambda names: [p if isinstance(p, Port) else Port(p) for p in names]   # noqa: E731
    return FunctionNode(id, function, inputs=ports(inputs), outputs=ports(outputs), **options)


def run(graph, inputs=None, workers=0):
    return Engine(graph, DEFAULT, workers=workers).run(inputs)


@pytest.mark.parametrize('workers', WORKERS)
def test_a_chain_runs_from_its_input_to_its_output(workers):
    graph = Chain(fn('a', lambda x: x + 1), fn('b', lambda x: x * 2), fn('c', lambda x: f'={x}')).build(DEFAULT)
    result = run(graph, {'in': 3}, workers)
    assert result.outputs == {'out': ['=8']} and result.last('out') == '=8'
    assert result.activations == 3 and result.rounds == {'a': 1, 'b': 1, 'c': 1} and result.limited == ()
    assert result.envelopes == 3          # the graph input and the two edges


def test_a_chain_may_begin_with_a_node_that_takes_nothing():
    graph = Chain(fn('a', lambda: 5, inputs=()), fn('b', lambda x: x + 1)).build(DEFAULT)
    assert graph.inputs == {} and run(graph).last('out') == 6
    with pytest.raises(GraphError, match='takes no input'):
        Chain(fn('a', lambda: 5, inputs=()), fn('b', lambda: 1, inputs=())).build(DEFAULT)
    with pytest.raises(GraphError, match='exactly one output'):
        Chain(fn('a', lambda: 5, inputs=(), outputs=('p', 'q')), fn('b', lambda x: x)).build(DEFAULT)


@pytest.mark.parametrize('workers', WORKERS)
def test_fan_out_and_fan_in_wait_for_every_branch(workers):
    seen = []
    builder = GraphBuilder().add(
        fn('split', lambda x: x), fn('left', lambda x: x + 1), fn('right', lambda x: (time.sleep(0.02), x * 10)[1]),
        fn('join', lambda parts: (seen.append(parts), sum(parts))[1], inputs=[Port('parts', many=True)]))
    builder.connect('split.y', 'left.x', 'value').connect('split.y', 'right.x', 'value')
    builder.connect('left.y', 'join.parts', 'value').connect('right.y', 'join.parts', 'value')
    graph = builder.input('in', 'split.x').output('out', 'join.y').compile(DEFAULT)
    result = run(graph, {'in': 2}, workers)
    assert result.outputs['out'] == [23] and seen == [[3, 20]]      # one activation, values in the order of the edges
    assert result.rounds['join'] == 1


def test_several_outputs_and_optional_inputs():
    def halves(x):
        return {'even': x} if x % 2 == 0 else {'odd': x}
    builder = GraphBuilder().add(
        fn('route', halves, outputs=('even', 'odd')),
        fn('e', lambda x: f'even {x}'), fn('o', lambda x: f'odd {x}'),
        fn('note', lambda x, hint='none': f'{x}/{hint}', inputs=[Port('x'), Port('hint', required=False)]))
    builder.connect('route.even', 'e.x', 'value').connect('route.odd', 'o.x', 'value').connect('e.y', 'note.x', 'value')
    graph = builder.input('in', 'route.x').output('even', 'e.y').output('odd', 'o.y').output('note', 'note.y').compile(DEFAULT)
    assert run(graph, {'in': 4}).outputs == {'even': ['even 4'], 'odd': [], 'note': ['even 4/none']}
    result = run(graph, {'in': 7})
    assert result.outputs == {'even': [], 'odd': ['odd 7'], 'note': []} and 'e' not in result.rounds


@pytest.mark.parametrize('workers', WORKERS)
def test_a_node_may_emit_many_times_and_the_next_node_runs_once_per_envelope(workers):
    def count(ctx, x):
        for i in range(x):
            ctx.emit('y', i)
    graph = Chain(fn('count', count, context=True), fn('square', lambda x: x * x)).build(DEFAULT)
    result = run(graph, {'in': 50}, workers)
    assert result.outputs['out'] == [i * i for i in range(50)]       # queued in order, one activation each
    assert result.rounds == {'count': 1, 'square': 50}


def test_a_loop_runs_until_the_node_stops_sending_back():
    def step(x):
        return {'again': x * 2} if x < 100 else {'done': x}
    builder = GraphBuilder().add(fn('step', step, outputs=('again', 'done')))
    builder.connect('step.again', 'step.x', 'value', back=True, max_rounds=50)
    graph = builder.input('in', 'step.x').output('out', 'step.done').compile(DEFAULT)
    result = run(graph, {'in': 3})
    assert result.outputs['out'] == [192] and result.rounds['step'] == 7 and result.limited == ()


@pytest.mark.parametrize('workers', WORKERS)
def test_a_back_edge_stops_at_its_round_limit(workers):
    work = fn('work', lambda x: x + 1)
    check = fn('check', lambda x: {'redo': x, 'seen': x}, outputs=('redo', 'seen'))
    builder = GraphBuilder().add(work, check).connect('work.y', 'check.x', 'value')
    builder.connect('check.redo', 'work.x', 'value', id='redo', back=True, max_rounds=3)
    graph = builder.input('in', 'work.x').output('out', 'check.seen').compile(DEFAULT)
    result = run(graph, {'in': 0}, workers)
    assert result.outputs['out'] == [1, 2, 3, 4]          # the first pass and three rounds through the back edge
    assert result.rounds == {'work': 4, 'check': 4} and result.limited == ('redo',)


def test_a_node_that_fires_on_any_input_takes_one_envelope_at_a_time():
    seen = []
    merge = fn('merge', lambda **got: (seen.append(got), next(iter(got.values())))[1],
               inputs=[Port('a', required=False), Port('b', required=False)], firing=Firing.ANY)
    builder = GraphBuilder().add(fn('one', lambda: 1, inputs=()), fn('two', lambda: 2, inputs=()), merge)
    graph = builder.connect('one.y', 'merge.a', 'value').connect('two.y', 'merge.b', 'value').output('out', 'merge.y').compile(DEFAULT)
    result = run(graph)
    assert seen == [{'a': 1}, {'b': 2}] and result.outputs['out'] == [1, 2] and result.rounds['merge'] == 2


def test_payloads_are_checked_against_the_port_that_sends_them_and_the_graph_input():
    graph = Chain(fn('a', lambda x: str(x), inputs=[Port('x', int)], outputs=[Port('y', int)]), fn('b', lambda x: x)).build(DEFAULT)
    with pytest.raises(RunError, match='graph input in: expected int') as caught:
        run(graph, {'in': 'three'})
    assert caught.value.node is None
    with pytest.raises(RunError, match='node a failed in round 1: y: expected int, got str') as caught:
        run(graph, {'in': 3})
    assert caught.value.node == 'a'
    with pytest.raises(RunError, match="no input \\['other'\\]"):
        run(graph, {'other': 1})


@pytest.mark.parametrize('workers', WORKERS)
def test_a_failing_node_ends_the_run_and_names_itself(workers):
    def boom(x):
        raise ZeroDivisionError('no')
    closed = []

    class Tracked(FunctionNode):
        def open(self, ctx):
            closed.append(('open', self.id))

        def close(self, ctx):
            closed.append(('close', self.id))
    graph = Chain(Tracked('a', lambda x: x, inputs=[Port('x')], outputs=[Port('y')]), fn('b', boom), fn('c', lambda x: x)).build(DEFAULT)
    with pytest.raises(RunError, match='node b failed in round 1: no') as caught:
        run(graph, {'in': 1}, workers)
    assert isinstance(caught.value.__cause__, ZeroDivisionError)
    assert closed == [('open', 'a'), ('close', 'a')]      # what was opened is closed even when the run fails


def test_what_a_node_returns_must_name_its_ports():
    two = lambda function: GraphBuilder().add(fn('a', function, outputs=('y', 'z'))).input('in', 'a.x').compile(DEFAULT)   # noqa: E731
    with pytest.raises(RunError, match="has no output port 'nope'"):
        run(two(lambda x: {'nope': 1, 'y': 2}), {'in': 1})
    with pytest.raises(RunError, match='must return a mapping'):
        run(two(lambda x: 5), {'in': 1})


def test_an_engine_runs_once_and_nodes_really_run_in_parallel():
    graph = Chain(fn('a', lambda x: x)).build(DEFAULT)
    engine = Engine(graph, DEFAULT)
    engine.run({'in': 1})
    with pytest.raises(RunError, match='already run'):
        engine.run({'in': 1})

    barrier = threading.Barrier(3, timeout=5)
    builder = GraphBuilder().add(fn('s', lambda: 0, inputs=()))
    for name in ('p', 'q', 'r'):
        builder.add(fn(name, lambda x: barrier.wait())).connect('s.y', f'{name}.x', 'value').output(name, f'{name}.y')
    result = run(builder.compile(DEFAULT), workers=3)     # the barrier only opens when all three run at once
    assert sorted(v[0] for v in result.outputs.values()) == [0, 1, 2]


def test_envelopes_record_where_they_came_from():
    seen = {}

    graph = Chain(fn('a', lambda x: x + 1), fn('b', lambda x: x)).build(DEFAULT)
    from interlacing.engine.scheduler import Scheduler

    class Spy(Scheduler):
        def place(self, feed, envelope):
            super().place(feed, envelope)
            seen[envelope.id] = envelope
    engine = Engine(graph, DEFAULT)
    engine._sched = Spy()
    engine.run({'in': 1})
    first, second = seen['input:in#1'], seen['a.y>b.x#1']
    assert first.cause == () and first.round == 0 and second.cause == ('input:in#1',) and second.round == 1
    assert second.body == 2 and len(second.digest) == 64 and second.seq == 1


def test_latest_delivery_keeps_only_the_newest():
    def burst(ctx):
        for i in range(5):
            ctx.emit('y', i)
    graph = GraphBuilder().add(fn('a', burst, inputs=(), context=True), fn('b', lambda x: x)) \
        .connect('a.y', 'b.x', 'latest').output('out', 'b.y').compile(DEFAULT)
    assert Engine(graph, DEFAULT).run().outputs['out'] == [4]


def test_layers_above_the_abstractions_import_only_what_is_below_them():
    import ast
    from pathlib import Path
    import sys
    src = Path(__file__).resolve().parents[1] / 'src' / 'interlacing'
    # nodes may use the engine (a graph as a node runs one); methods may use the nodes they place in a graph
    allowed = {'engine': {'base', 'engine'}, 'nodes': {'base', 'engine', 'nodes'}, 'protocols': {'base', 'protocols'},
               'methods': {'base', 'nodes', 'methods'}, 'models': {'models'}, 'base': {'base'},
               'spec': {'base', 'engine', 'nodes', 'protocols', 'methods', 'models', 'blocks', 'spec'},
               'blocks': {'base', 'nodes', 'protocols', 'methods', 'blocks'}}
    for package, may in allowed.items():
        for path in (src / package).glob('*.py'):
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                if isinstance(node, ast.ImportFrom) and node.level:
                    target = (node.module or '').split('.')[0] if node.level == 2 else package
                    assert node.level <= 2 and target in may, f'{package}/{path.name}:{node.lineno}'
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    names = [node.module] if isinstance(node, ast.ImportFrom) else [a.name for a in node.names]
                    assert all(n.split('.')[0] in sys.stdlib_module_names for n in names), f'{package}/{path.name}:{node.lineno}'


@pytest.mark.parametrize('payload, expected_hashes', [('original', 2), (['original'], 4)])
def test_fanout_hashes_the_final_body_and_shares_only_within_one_emit(monkeypatch, payload, expected_hashes):
    from interlacing.base import Protocol, digest
    from interlacing.engine import Transport, scheduler

    class Decorate(Protocol):
        name = 'decorate'

        def encode(self, value, source):
            return 'encoded'

    class Carry(Transport):
        def carry(self, edge, body):
            return 'transported' if edge == 'transport' else body

    def send(ctx, x):
        for _ in range(2):
            ctx.emit('y', payload)

    protocols = DEFAULT.copy()
    protocols.register(Decorate())
    builder = GraphBuilder().add(fn('source', send, context=True)).input('in', 'source.x')
    for name, protocol in [('a', 'value'), ('b', 'decorate'), ('c', 'value'), ('transport', 'value')]:
        builder.add(fn(name, lambda x: x)).connect('source.y', f'{name}.x', protocol, id=name).output(name, f'{name}.y')

    hashed, posted = [], []
    def counted(body):
        hashed.append(body)
        return digest(body)

    engine = Engine(builder.compile(protocols), protocols, transport=Carry())
    original = engine._post
    def capture(feed, envelope, act):
        posted.append(envelope)
        return original(feed, envelope, act)

    monkeypatch.setattr(scheduler, 'digest', counted)
    monkeypatch.setattr(engine, '_post', capture)
    result = engine.run({'in': 1})
    assert hashed.count(payload) == expected_hashes
    assert result.outputs == {'a': [payload] * 2, 'c': [payload] * 2,
                              'b': ['encoded'] * 2, 'transport': ['transported'] * 2}
    assert all(e.digest == digest(e.body) for e in posted)
    assert len({e.id for e in posted}) == len(posted)
