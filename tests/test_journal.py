"""A journaled run can be taken up again after it was cut off, and can continue under a changed graph."""
import json
import subprocess
import sys
import textwrap
import threading

import pytest

from interlacing.base import GraphBuilder, GraphError, Node, Port
from interlacing.engine import Engine, Journal, JournalError, RunError
from interlacing.methods import Chain, Loop
from interlacing.nodes import FunctionNode, GraphNode
from interlacing.protocols import DEFAULT, standard


def fn(id, function, inputs=('x',), outputs=('y',), **options):
    ports = lambda names: [p if isinstance(p, Port) else Port(p) for p in names]   # noqa: E731
    return FunctionNode(id, function, inputs=ports(inputs), outputs=ports(outputs), **options)


class Flaky:
    """A function that fails until it is told to work, and counts how often it was called."""

    def __init__(self, function):
        self.function, self.broken, self.calls = function, True, 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        if self.broken:
            raise OSError('the machine went away')
        return self.function(*args, **kwargs)


def add_one(x):
    return x + 1


def double(x):
    return x * 2


def records(path):
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]


@pytest.mark.parametrize('workers', [0, 1, 4])
def test_a_journaled_run_gives_the_same_result_and_a_finished_one_is_not_run_again(tmp_path, workers):
    calls = []
    graph = lambda: Chain(fn('a', lambda x: (calls.append('a'), x + 1)[1]), fn('b', lambda x: (calls.append('b'), x * 2)[1])).build(DEFAULT)  # noqa: E731
    plain = Engine(graph(), DEFAULT).run({'in': 3})
    path = tmp_path / 'run.jsonl'
    result = Engine(graph(), DEFAULT, workers=workers, journal=Journal(path)).run({'in': 3})
    assert result.outputs == plain.outputs == {'out': [8]} and result.complete
    kinds = [r['kind'] for r in records(path)]
    assert kinds == ['run', 'act', 'act', 'end']
    # a's record may come after b's: b can take what a sent before a's activation is over (see _orphans)
    act = next(r for r in records(path) if r.get('node') == 'a')
    assert act['took'] == ['input:in#1'] and act['sent'] == [['a.y>b.x', 1, 1, ['v', 4], ['input:in#1']]]
    calls.clear()
    again = Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    assert again.outputs == {'out': [8]} and again.rounds == {'a': 1, 'b': 1} and again.activations == 2 and again.envelopes == 2
    assert calls == [] and again.complete and [r['kind'] for r in records(path)] == kinds


def test_what_was_finished_is_not_done_again_and_what_was_cut_off_is(tmp_path):
    path = tmp_path / 'run.jsonl'
    first, second, third = Flaky(add_one), Flaky(double), Flaky(add_one)
    first.broken = False
    graph = lambda: Chain(fn('a', first), fn('b', second), fn('c', third)).build(DEFAULT)   # noqa: E731
    with pytest.raises(RunError, match='node b failed in round 1: the machine went away'):
        Engine(graph(), DEFAULT, journal=Journal(path)).run({'in': 1})
    assert [r['kind'] for r in records(path)] == ['run', 'act']         # a finished; b left nothing; no end
    second.broken = False
    with pytest.raises(RunError, match='node c failed'):
        Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    third.broken = False
    result = Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    assert result.outputs == {'out': [5]} and result.complete
    assert (first.calls, second.calls, third.calls) == (1, 2, 2)
    assert result.rounds == {'a': 1, 'b': 1, 'c': 1} and result.activations == 3 and result.envelopes == 3


def test_a_backend_without_paths_can_resume_and_discard_a_child_journal():
    class MemoryJournal:
        def __init__(self):
            self.records = []
            self.children = {}

        def read(self):
            return self.records.copy()

        def write(self, record):
            self.records.append(record)

        def close(self):
            pass

        def reset(self):
            self.records.clear()

        def inner(self, node, round):
            key = (node, round)
            if key not in self.children:
                self.children[key] = MemoryJournal()
            return self.children[key]

        def discard(self):
            self.reset()
            self.children.clear()

        def prune_empty_inner(self):
            self.children = {key: child for key, child in self.children.items() if child.records or child.children}

    first, second = Flaky(add_one), Flaky(double)
    first.broken = False

    def build():
        inner = Chain(fn('first', first), fn('second', second)).build(DEFAULT)
        return Chain(GraphNode('block', inner)).build(DEFAULT)

    journal = MemoryJournal()
    with pytest.raises(RunError, match='node second failed'):
        Engine(build(), DEFAULT, journal=journal).run({'in': 3})
    child = journal.children[('block', 1)]
    assert [record['kind'] for record in child.records] == ['run', 'act']

    second.broken = False
    result = Engine(build(), DEFAULT, journal=journal).resume()
    assert result.outputs == {'out': [8]}
    assert (first.calls, second.calls) == (1, 2)
    assert journal.children == {}

    result = Engine(build(), DEFAULT, journal=journal).go({'in': 5}, fresh=True)
    assert result.outputs == {'out': [12]}
    assert journal.children == {}


def test_a_run_survives_the_process_being_killed(tmp_path):
    (tmp_path / 'shared.py').write_text(textwrap.dedent('''
        import os, sys
        from interlacing.base import Port
        from interlacing.methods import Chain
        from interlacing.nodes import FunctionNode
        from interlacing.protocols import DEFAULT

        here = os.path.dirname(__file__)

        def count(ctx, x):
            for i in range(x):
                ctx.emit('y', i)

        def square(x):
            with open(os.path.join(here, 'squared.txt'), 'a') as file:
                file.write(f'{x}\\n')
            if x == 3 and not os.path.exists(os.path.join(here, 'second-life')):
                os._exit(9)                      # dies in the middle of an activation, without any cleanup
            return x * x

        def graph():
            node = lambda id, f, **o: FunctionNode(id, f, inputs=[Port('x')], outputs=[Port('y')], **o)
            return Chain(node('count', count, context=True), node('square', square)).build(DEFAULT)
    '''), encoding='utf-8')
    script = ('import sys; sys.path.insert(0, sys.argv[1]); import shared\n'
              'from interlacing.engine import Engine, Journal\n'
              'from interlacing.protocols import DEFAULT\n'
              'engine = Engine(shared.graph(), DEFAULT, journal=Journal(sys.argv[1] + "/run.jsonl"))\n'
              'result = engine.run({"in": 6}) if sys.argv[2] == "run" else engine.resume()\n'
              'print(result.outputs["out"], result.rounds)\n')
    died = subprocess.run([sys.executable, '-c', script, str(tmp_path), 'run'], capture_output=True, text=True)
    assert died.returncode == 9
    assert [r['node'] for r in records(tmp_path / 'run.jsonl')[1:]] == ['count', 'square', 'square', 'square']
    (tmp_path / 'second-life').write_text('')
    alive = subprocess.run([sys.executable, '-c', script, str(tmp_path), 'resume'], capture_output=True, text=True)
    assert alive.returncode == 0, alive.stderr
    assert alive.stdout.strip() == "[0, 1, 4, 9, 16, 25] {'count': 1, 'square': 6}"
    assert (tmp_path / 'squared.txt').read_text().split() == ['0', '1', '2', '3', '3', '4', '5']   # only 3 was seen twice


def test_what_a_cut_off_node_had_sent_is_not_dealt_with_twice(tmp_path):
    path = tmp_path / 'run.jsonl'
    seen, handled, attempt = [], threading.Event(), [0]

    def send(ctx):
        attempt[0] += 1
        ctx.emit('y', 'first')
        if attempt[0] == 1:
            assert handled.wait(5)                  # b has dealt with 'first' and its activation is in the journal
            raise OSError('cut off')
        ctx.emit('y', 'second')

    def handle(x):
        seen.append(x)
        handled.set()
    graph = lambda: GraphBuilder().add(fn('a', send, inputs=(), context=True), fn('b', handle, outputs=())) \
        .connect('a.y', 'b.x', 'value').compile(DEFAULT)   # noqa: E731
    with pytest.raises(RunError, match='cut off'):
        Engine(graph(), DEFAULT, workers=2, journal=Journal(path)).run()
    assert [r['node'] for r in records(path)[1:]] == ['b'] and seen == ['first']
    result = Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    assert seen == ['first', 'second'] and result.rounds == {'a': 1, 'b': 2} and result.complete
    assert Engine(graph(), DEFAULT, journal=Journal(path)).resume().rounds == {'a': 1, 'b': 2}


class Counter(Node):
    """Adds up everything it has ever received: state of its own, which must survive a restart."""

    kind = 'counter'
    inputs = (Port('x', int),)
    outputs = (Port('y', int),)

    def __init__(self, id):
        super().__init__(id)
        self.total = 0

    def run(self, ctx, inputs):
        self.total += inputs['x']
        return {'y': self.total}

    def snapshot(self):
        return {'total': self.total}

    def restore(self, state):
        self.total = state['total']


def test_a_nodes_own_state_is_restored(tmp_path):
    path = tmp_path / 'run.jsonl'
    last = Flaky(lambda x: x)
    last.broken = False

    def fail_on_three(x):
        if x == 6 and last.broken:
            raise OSError('cut off')
        return x
    last.broken = True
    count = lambda ctx: [ctx.emit('y', i) for i in (1, 2, 3, 4)] and None   # noqa: E731
    graph = lambda: Chain(fn('numbers', count, inputs=(), outputs=[Port('y', int)], context=True), Counter('sum'),
                          fn('check', fail_on_three)).build(DEFAULT)   # noqa: E731
    with pytest.raises(RunError, match='cut off'):
        Engine(graph(), DEFAULT, journal=Journal(path)).run()
    last.broken = False
    result = Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    assert result.outputs == {'out': [1, 3, 6, 10]} and result.rounds['sum'] == 4


def test_round_limits_count_across_a_restart(tmp_path):
    path = tmp_path / 'run.jsonl'
    step = Flaky(add_one)
    step.broken = False

    def body(x):
        if x == 2 and step.calls == 2:
            step.calls += 1
            raise OSError('cut off')
        return step(x)
    graph = lambda: Loop(fn('inc', body), until=lambda x: False, max_rounds=5).build(DEFAULT)   # noqa: E731
    with pytest.raises(RunError, match='cut off'):
        Engine(graph(), DEFAULT, journal=Journal(path)).run({'in': 0})
    result = Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    assert result.outputs == {'out': [5]} and result.rounds == {'inc': 5, 'until': 5}


def test_a_stream_and_an_artifact_come_back_from_the_journal(tmp_path):
    from interlacing.protocols import DirectoryStore
    path, protocols = tmp_path / 'run.jsonl', standard(DirectoryStore(tmp_path / 'store'))
    reader = Flaky(lambda x: ''.join(x))
    keeper = Flaky(lambda x: x.upper())
    graph = lambda: GraphBuilder().add(fn('say', lambda ctx: [ctx.emit('y', w) for w in 'abc'] and None, inputs=(), context=True),
                                       fn('read', reader), fn('keep', keeper)) \
        .connect('say.y', 'read.x', 'stream').connect('read.y', 'keep.x', 'artifact').output('out', 'keep.y').compile(protocols)   # noqa: E731
    with pytest.raises(RunError, match='node read failed'):
        Engine(graph(), protocols, journal=Journal(path)).run()
    assert records(path)[1]['sent'] == [['say.y>read.x', 1, 1, ['s', [['v', 'a'], ['v', 'b'], ['v', 'c']]], []]]
    reader.broken = False
    with pytest.raises(RunError, match='node keep failed'):
        Engine(graph(), protocols, journal=Journal(path)).resume()
    assert records(path)[2]['sent'][0][3][0] == 'r'              # the artifact is journaled as a reference
    keeper.broken = False
    assert Engine(graph(), protocols, journal=Journal(path)).resume().outputs == {'out': ['ABC']}


def test_a_stopped_run_is_incomplete_and_can_go_on(tmp_path):
    path = tmp_path / 'run.jsonl'
    engines = []

    def stop_at_two(x):
        if x == 2 and len(engines) == 1:
            engines[0].stop()
        return x
    count = lambda ctx: [ctx.emit('y', i) for i in range(5)] and None   # noqa: E731
    graph = lambda: Chain(fn('numbers', count, inputs=(), context=True), fn('pass', stop_at_two)).build(DEFAULT)   # noqa: E731
    engines.append(Engine(graph(), DEFAULT, journal=Journal(path)))
    stopped = engines[0].run()
    assert stopped.outputs == {'out': [0, 1, 2]} and not stopped.complete and records(path)[-1]['kind'] == 'act'
    engines.append(Engine(graph(), DEFAULT, journal=Journal(path)))
    result = engines[1].resume()
    assert result.outputs == {'out': [0, 1, 2, 3, 4]} and result.complete and records(path)[-1]['kind'] == 'end'


def test_what_is_journaled_must_be_plain_data(tmp_path):
    with pytest.raises(RunError, match='node a failed in round 1: cannot be journaled'):
        Engine(Chain(fn('a', lambda x: object()), fn('b', lambda x: 1)).build(DEFAULT), DEFAULT, journal=Journal(tmp_path / 'a.jsonl')).run({'in': 1})
    with pytest.raises(RunError, match="the graph's inputs cannot be journaled"):
        Engine(Chain(fn('a', lambda x: 1)).build(DEFAULT), DEFAULT, journal=Journal(tmp_path / 'b.jsonl')).run({'in': object()})


def test_a_journal_is_for_one_run_and_one_graph_unless_the_change_is_asked_for(tmp_path):
    path = tmp_path / 'run.jsonl'
    graph = lambda: Chain(fn('a', add_one), fn('b', double)).build(DEFAULT)   # noqa: E731
    with pytest.raises(RunError, match='resume needs a journal'):
        Engine(graph(), DEFAULT).resume()
    with pytest.raises(RunError, match='nothing to resume'):
        Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    Engine(graph(), DEFAULT, journal=Journal(path)).run({'in': 1})
    with pytest.raises(RunError, match='already holds a run'):
        Engine(graph(), DEFAULT, journal=Journal(path)).run({'in': 1})
    other = Chain(fn('a', add_one), fn('c', double)).build(DEFAULT)
    with pytest.raises(RunError, match='written under another graph .* pass change=True'):
        Engine(other, DEFAULT, journal=Journal(path)).resume()


def test_a_run_continues_under_a_changed_graph(tmp_path):
    path = tmp_path / 'run.jsonl'
    review = Flaky(lambda x: f'reviewed {x}')
    old = Chain(fn('draft', add_one), fn('review', review)).build(DEFAULT)
    with pytest.raises(RunError, match='node review failed'):
        Engine(old, DEFAULT, journal=Journal(path)).run({'in': 1})

    # the change: review is replaced by two checks side by side; draft, already finished, stays as it is
    builder = GraphBuilder.of(old).remove('review').add(fn('check', lambda x: f'checked {x}'), fn('audit', lambda x: f'audited {x}'))
    builder.connect('draft.y', 'check.x', 'value', id='draft.y>review.x')      # takes over the edge, and what waits on it
    builder.connect('draft.y', 'audit.x', 'value').output('out', 'check.y').output('audit', 'audit.y')
    new = builder.compile(DEFAULT)
    assert new.digest != old.digest and 'review' not in new and old.node('review')        # the old graph is untouched
    result = Engine(new, DEFAULT, journal=Journal(path)).resume(change=True)
    assert result.outputs == {'out': ['checked 2'], 'audit': []}      # audit's edge is new: draft does not run again for it
    assert result.rounds == {'draft': 1, 'check': 1} and result.complete
    change = [r for r in records(path) if r['kind'] == 'change']
    assert change == [{'schema_version': 1, 'kind': 'change', 'from': old.digest, 'graph': new.digest, 'dropped': 0}]
    assert Engine(new, DEFAULT, journal=Journal(path)).resume().outputs == {'out': ['checked 2'], 'audit': []}

    # dropping an edge drops what was waiting on it, and says how much
    path = tmp_path / 'second.jsonl'
    review.broken = True
    with pytest.raises(RunError, match='node review failed'):
        Engine(old, DEFAULT, journal=Journal(path)).run({'in': 1})
    cut = GraphBuilder.of(old).remove('review').output('out', 'draft.y').compile(DEFAULT)
    assert Engine(cut, DEFAULT, journal=Journal(path)).resume(change=True).outputs == {'out': []}
    assert records(path)[-2]['dropped'] == 1 and records(path)[-1]['kind'] == 'end'


def test_a_builder_made_from_a_graph_can_take_parts_out():
    graph = Chain(fn('a', add_one), fn('b', double), fn('c', add_one)).build(DEFAULT)
    same = GraphBuilder.of(graph).compile(DEFAULT)
    assert same.digest == graph.digest and same is not graph
    builder = GraphBuilder.of(graph).disconnect('b.y>c.x')
    with pytest.raises(GraphError, match='c.x is required and nothing feeds it'):
        builder.compile(DEFAULT)
    shorter = GraphBuilder.of(graph).remove('c').output('out', 'b.y').compile(DEFAULT)
    assert [n.id for n in shorter.nodes] == ['a', 'b'] and [e.id for e in shorter.edges] == ['a.y>b.x']
    assert Engine(shorter, DEFAULT).run({'in': 1}).outputs == {'out': [4]}
    for wrong, call in (('no node', lambda b: b.remove('z')), ('no edge', lambda b: b.disconnect('z'))):
        with pytest.raises(GraphError, match=wrong):
            call(GraphBuilder.of(graph))


def test_inspection_reads_only_the_complete_prefix_present_at_open(tmp_path):
    path = tmp_path / 'run.jsonl'
    before = b'{"kind":"run"}\n{"kind":"act"'
    after = b'}\n{"kind":"end"}\n'
    path.write_bytes(before)
    journal = Journal(path)

    records = journal.inspect()
    assert next(records) == {'kind': 'run'}
    with path.open('ab') as writer:
        writer.write(after)
    assert list(records) == []
    assert path.read_bytes() == before + after
    assert list(journal.inspect()) == [{'kind': 'run'}, {'kind': 'act'}, {'kind': 'end'}]


def test_a_line_cut_short_is_dropped_and_a_damaged_one_is_refused(tmp_path):
    path = tmp_path / 'run.jsonl'
    second = Flaky(double)
    graph = lambda: Chain(fn('a', add_one), fn('b', second)).build(DEFAULT)   # noqa: E731
    with pytest.raises(RunError):
        Engine(graph(), DEFAULT, journal=Journal(path)).run({'in': 1})
    whole = path.read_bytes()
    path.write_bytes(whole + b'{"kind":"act","node":"b","rou')           # the process died while writing
    second.broken = False
    assert Engine(graph(), DEFAULT, journal=Journal(path)).resume().outputs == {'out': [4]}
    assert path.read_bytes().startswith(whole) and b'"rou{' not in path.read_bytes() and records(path)[-1]['kind'] == 'end'
    lines = path.read_bytes().splitlines(keepends=True)
    path.write_bytes(lines[0] + b'not json\n' + b''.join(lines[1:]))
    with pytest.raises(JournalError, match='line 2 is damaged'):
        Journal(path).read()
    journal = Journal(tmp_path / 'deep' / 'dir' / 'j.jsonl', sync=True)
    journal.write({'kind': 'end'})
    journal.close()
    assert journal.read() == [{'kind': 'end'}]


@pytest.mark.parametrize('workers', [0, 2])
def test_an_interrupt_inside_a_node_passes_through_and_leaves_the_run_resumable(tmp_path, workers):
    path = tmp_path / 'run.jsonl'
    second = Flaky(double)

    def interrupted(x):
        if second.broken:
            raise KeyboardInterrupt
        return second(x)
    graph = lambda: Chain(fn('a', add_one), fn('b', interrupted)).build(DEFAULT)   # noqa: E731
    with pytest.raises(KeyboardInterrupt):
        Engine(graph(), DEFAULT, workers=workers, journal=Journal(path)).run({'in': 1})
    assert [r['kind'] for r in records(path)] == ['run', 'act']          # no end: the run is not over
    second.broken = False
    assert Engine(graph(), DEFAULT, journal=Journal(path)).resume().outputs == {'out': [4]}


def test_a_receiver_journaled_before_its_sender_is_not_run_again_when_the_sender_is(tmp_path):
    """A receiver may finish, and be journaled, before the activation that sent to it is: when the run stops
    in between, the sender runs again on resume and what it sends again is not delivered a second time."""
    calls = []
    graph = lambda: Chain(fn('a', lambda x: (calls.append('a'), x + 1)[1]), fn('b', lambda x: (calls.append('b'), x * 2)[1])).build(DEFAULT)  # noqa: E731
    path = tmp_path / 'run.jsonl'
    Engine(graph(), DEFAULT, journal=Journal(path)).run({'in': 3})
    kept = [r for r in records(path) if r['kind'] == 'run' or r.get('node') == 'b']     # a's record was lost
    path.write_text(''.join(json.dumps(r) + '\n' for r in kept), encoding='utf-8')
    calls.clear()
    again = Engine(graph(), DEFAULT, journal=Journal(path)).resume()
    assert calls == ['a'] and again.outputs == {'out': [8]} and again.complete
