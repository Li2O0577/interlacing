"""A block: a designed graph kept as a unit, used as often as needed, joined with others, kept in a file."""
import json

import pytest

from interlacing import SQLiteJournal
from interlacing.base import GraphError, Port
from interlacing.blocks import Block, par, seq
from interlacing.engine import Engine, Journal, RunError
from interlacing.methods import Chain, Fan, Loop
from interlacing.nodes import FunctionNode, GraphNode
from interlacing.protocols import DEFAULT
from interlacing.runs import Run
from interlacing.spec import Catalog, SpecError, block, dump, load

HERE = __name__
SEEN = []
BROKEN = []


def fn(function):
    return lambda id: FunctionNode(id, function, inputs=[Port('x')], outputs=[Port('y')])


def add_one(x):
    return x + 1


def double(x):
    return x * 2


def total(x):
    return sum(x)


def fragile(x):
    SEEN.append(x)
    if BROKEN:
        raise OSError('not yet')
    return x + 100


def never_even(x):
    return x


def even(value):
    return value % 2 == 0


# ---- used as often as needed -----------------------------------------------------------------------------

def test_every_use_of_a_block_is_a_node_of_its_own():
    counter = Block(lambda: Loop(fn(add_one)('inc'), until=lambda x: x % 5 == 0, max_rounds=10), name='to-five')
    first, second = counter('first'), counter('second')
    assert isinstance(first, GraphNode) and first.graph is not second.graph and first.graph.digest == second.graph.digest
    assert first.graph.node('inc') is not second.graph.node('inc')
    assert Engine(Chain(first, fn(add_one)('step'), second).build(DEFAULT), DEFAULT).run({'in': 1}).last('out') == 10
    assert [node.id for node in counter.times(3)] == ['to-five-1', 'to-five-2', 'to-five-3']
    assert [node.id for node in counter.times(2, 'pass')] == ['pass-1', 'pass-2'] and counter().id == 'to-five-8'
    assert counter.graph().digest == first.graph.digest and set(first.inputs) == {'in'} and set(first.outputs) == {'out'}
    with pytest.raises(GraphError, match='must return a graph or a method'):
        Block(lambda: 3)('x')
    with pytest.raises(GraphError, match='whole number of at least 1'):
        counter.times(0)
    with pytest.raises(GraphError, match='made from a function'):
        Block(Chain(fn(add_one)('a')))


@pytest.mark.parametrize('workers', [0, 3])
def test_blocks_are_joined_in_a_row_and_side_by_side_and_nest(workers):
    step = seq(fn(add_one), fn(double), name='step')                       # (x + 1) * 2
    assert Engine(Chain(step('s')).build(DEFAULT), DEFAULT).run({'in': 1}).last('out') == 4
    panel = par(step, fn(add_one), step, name='panel')
    assert Engine(Chain(panel('p')).build(DEFAULT), DEFAULT, workers=workers).run({'in': 1}).last('out') == [4, 2, 4]
    study = seq(step, panel, fn(total), step, name='study', workers=workers)   # 1 -> 4 -> [10, 5, 10] -> 25 -> 52
    result = Engine(Fan(*study.times(3)).build(DEFAULT), DEFAULT, workers=workers).run({'in': 1})
    assert result.last('out') == [52, 52, 52] and result.rounds == {'split': 1, 'study-1': 1, 'study-2': 1, 'study-3': 1, 'join': 1}
    inner = study('one').graph
    assert [node.id for node in inner.nodes] == ['s1', 's2', 's3', 's4'] and inner.node('s2').graph.node('p1').kind == 'graph'


def test_what_a_joined_block_refuses():
    existing = fn(add_one)('made-already')
    once = seq(existing, fn(double))
    assert Engine(Chain(once('a')).build(DEFAULT), DEFAULT).run({'in': 1}).last('out') == 4
    with pytest.raises(GraphError, match='can be used only once'):
        once('b')
    for wrong, why in ((lambda: seq(), 'needs at least one part'), (lambda: seq(3)('x'), 'a part is a block'),
                       (lambda: par(lambda id: 3)('x'), 'a part must make a node')):
        with pytest.raises(GraphError, match=why):
            wrong()


# ---- the run inside belongs to the run outside ------------------------------------------------------------

def test_what_happens_inside_a_block_is_told_to_the_outer_observer():
    seen = []
    step = seq(fn(add_one), fn(double), name='step')
    Engine(Chain(seq(step, name='outer')('o')).build(DEFAULT), DEFAULT, observe=seen.append).run({'in': 1})
    inside = [(e['event'], e.get('node'), e.get('within')) for e in seen if e['event'] in ('started', 'ended')]
    assert inside == [('started', 'o', None), ('started', 's1', ['o']), ('started', 's1', ['o', 's1']), ('started', 's2', ['o', 's1']),
                      ('ended', None, ['o', 's1']), ('ended', None, ['o']), ('ended', None, None)]


@pytest.mark.parametrize("backend", [Journal, SQLiteJournal])
def test_after_a_restart_a_block_goes_on_inside_instead_of_starting_over(tmp_path, backend):
    def build():
        return Chain(seq(fn(fragile), fn(fragile), fn(fragile), name='three')('block'), fn(add_one)('after')).build(DEFAULT)
    SEEN.clear()
    BROKEN.clear()

    def break_on_second(event):
        if event['event'] == 'finished' and event.get('within') == ['block'] and event['node'] == 's1':
            BROKEN.append(True)
    journal = backend(tmp_path / 'journal')
    with pytest.raises(RunError, match='node block failed in round 1: node s2 failed in round 1: not yet'):
        Engine(build(), DEFAULT, journal=journal, observe=break_on_second).run({'in': 1})
    assert SEEN == [1, 101] and journal.inner('block', 1).read()[-1]['kind'] == 'act'
    BROKEN.clear()
    result = Engine(build(), DEFAULT, journal=backend(journal.path)).resume()
    assert result.last('out') == 302 and SEEN == [1, 101, 101, 201]         # s1 did not run again
    assert journal.inner('block', 1).read() == []
    assert Engine(build(), DEFAULT, journal=backend(journal.path)).resume().last('out') == 302


@pytest.mark.parametrize("backend", [Journal, SQLiteJournal])
def test_finished_blocks_keep_separate_history_after_their_journals_are_removed(tmp_path, backend):
    from interlacing.inspection import explain_records
    inner = seq(fn(add_one), fn(double), name='shared')
    graph = Fan(inner('left'), inner('right')).build(DEFAULT)
    journal = backend(tmp_path / 'journal')
    assert Engine(graph, DEFAULT, workers=2, journal=journal).run({'in': 3}).complete
    assert journal.inner('left', 1).read() == journal.inner('right', 1).read() == []

    provenance = explain_records(dump(graph), journal.inspect(), 'done', [], timeline=True)['provenance']
    left, right = provenance['children']
    assert left['scope_id'] and right['scope_id'] and left['scope_id'] != right['scope_id']
    assert {left['parent_event_seq'], right['parent_event_seq']} == {
        item['event_seq'] for item in provenance['activations'] if item['node'] in ('left', 'right')}
    for child in (left, right):
        first, second = child['activations']
        assert (first['node'], second['node']) == ('s1', 's2')
        assert second['reads'][0]['source']['event_seq'] == first['event_seq']


def test_a_time_limit_applies_to_each_node_inside_a_block_and_problems_inside_are_seen_outside(tmp_path):
    import time
    slow = fn(lambda x: time.sleep(0.2) or x)        # each well inside the limit, all three together over it
    assert Engine(Chain(seq(slow, slow, slow)('b')).build(DEFAULT), DEFAULT, time_limit=0.5).run({'in': 1}).last('out') == 1
    stuck = fn(lambda x: time.sleep(3) or x)
    with pytest.raises(RunError, match='node b failed in round 1: node s2 ran longer than 0.6 seconds'):
        Engine(Chain(seq(slow, stuck)('b')).build(DEFAULT), DEFAULT, time_limit=0.6).run({'in': 1})
    warned = lambda id: FunctionNode(id, never_even, inputs=[Port('x')], outputs=[Port('y', check=even, invalid='warn')])   # noqa: E731
    result = Engine(Chain(seq(warned)('b')).build(DEFAULT), DEFAULT, journal=Journal(tmp_path / 'j')).run({'in': 3})
    assert result.problems == [{'node': 'b/s1', 'round': 1, 'port': 'y', 'problem': 'even does not hold', 'handled': 'warned'}]
    assert Engine(Chain(seq(warned)('b')).build(DEFAULT), DEFAULT, journal=Journal(tmp_path / 'j')).resume().problems == result.problems


# ---- kept in a file ---------------------------------------------------------------------------------------

def node(id, function, **more):
    return {'id': id, 'kind': 'function', 'function': f'{HERE}.{function}', 'inputs': ['x'], 'outputs': ['y'], **more}


def models(name):
    from interlacing.models import ChatResult, FakeProvider
    model = FakeProvider(lambda system, messages, tools: ChatResult(text=f'{name}:{messages[-1]["content"]}'))
    model.name = name
    return model


def test_a_block_kept_in_a_file_is_used_by_other_graphs(tmp_path):
    (tmp_path / 'blocks').mkdir()
    design = {'method': 'chain', 'nodes': [node('inc', 'add_one'), node('dbl', 'double')]}
    (tmp_path / 'blocks' / 'step.json').write_text(json.dumps(design), encoding='utf-8')
    catalog = Catalog(allow=[HERE], files=tmp_path)
    outer = {'method': 'chain', 'nodes': [{'id': 'first', 'kind': 'block', 'file': 'blocks/step.json'},
                                         {'id': 'second', 'kind': 'block', 'file': 'blocks/step.json'}]}
    graph = load(outer, catalog)
    assert Engine(graph, DEFAULT).run({'in': 1}).last('out') == 10
    written = dump(graph)                                           # the design is in what is written: no file needed again
    assert [n['kind'] for n in written['nodes']] == ['graph', 'graph'] and len(written['graphs']) == 1
    assert load(written, Catalog(allow=[HERE])).digest == graph.digest
    several = load({'method': 'fan', 'nodes': [{'id': 'step', 'kind': 'block', 'file': 'blocks/step.json', 'times': 3}, node('inc', 'add_one')]}, catalog)
    assert sorted(n.id for n in several.nodes) == ['inc', 'join', 'split', 'step-1', 'step-2', 'step-3']
    assert Engine(several, DEFAULT).run({'in': 1}).last('out') == [4, 4, 4, 2]
    in_place = load({'nodes': [{'id': 'b', 'kind': 'block', 'graph': design}], 'inputs': {'in': 'b.in'}, 'outputs': {'out': 'b.out'}}, catalog)
    assert Engine(in_place, DEFAULT).run({'in': 2}).last('out') == 6
    for wrong, why in (({'id': 'b', 'kind': 'block'}, 'needs a file, or a graph written in place'),
                       ({'id': 'b', 'kind': 'block', 'file': 'blocks/none.json'}, 'cannot be read'),
                       ({'id': 'b', 'kind': 'block', 'file': '../x.json'}, 'outside'),
                       ({'id': 'b', 'kind': 'block', 'file': 'blocks/step.json', 'times': 0}, 'times is a whole number')):
        with pytest.raises(SpecError, match=why):
            load({'method': 'chain', 'nodes': [wrong]}, catalog)
    with pytest.raises(SpecError, match='allows no files'):
        load(outer, Catalog(allow=[HERE]))


def test_a_use_of_a_block_may_replace_the_blocks_own_defaults(tmp_path):
    design = {'defaults': {'agent': {'model': 'fake:small', 'max_steps': 5}}, 'method': 'chain',
              'nodes': [{'id': 'a', 'kind': 'agent'}, {'id': 'b', 'kind': 'agent', 'model': 'fake:fixed'}]}
    (tmp_path / 'pair.json').write_text(json.dumps(design), encoding='utf-8')
    catalog = Catalog(models=models, files=tmp_path)
    graph = load({'method': 'chain', 'nodes': [{'id': 'plain', 'kind': 'block', 'file': 'pair.json'},
                                              {'id': 'big', 'kind': 'block', 'file': 'pair.json', 'defaults': {'agent': {'model': 'fake:large'}}}]}, catalog)
    inner = lambda id: graph.node(id).graph   # noqa: E731
    assert (inner('plain').node('a').model.name, inner('plain').node('b').model.name) == ('fake:small', 'fake:fixed')
    assert (inner('big').node('a').model.name, inner('big').node('b').model.name, inner('big').node('a').max_steps) == ('fake:large', 'fake:fixed', 5)
    assert Engine(graph, DEFAULT).run({'in': 'hi'}).last('out') == 'fake:fixed:fake:large:fake:fixed:fake:small:hi'


def test_a_block_is_made_from_a_file_in_python_too_and_a_person_inside_a_block_is_waited_for(tmp_path):
    design = {'method': 'chain', 'nodes': [node('inc', 'add_one'), {'id': 'editor', 'kind': 'human'}, node('dbl', 'double')]}
    (tmp_path / 'approve.json').write_text(json.dumps({'method': 'chain', 'nodes': [node('inc', 'add_one'), node('dbl', 'double')]}), encoding='utf-8')
    step = block(tmp_path / 'approve.json', Catalog(allow=[HERE]))
    assert step.name == 'approve' and Engine(Chain(*step.times(2)).build(DEFAULT), DEFAULT).run({'in': 1}).last('out') == 10
    with pytest.raises(SpecError, match='not in a module this catalog allows'):
        block(tmp_path / 'approve.json')
    run = Run.create(tmp_path / 'run', {'method': 'chain', 'nodes': [{'id': 'gate', 'kind': 'block', 'graph': design}, node('after', 'add_one')]}, allow=[HERE])
    state = run.start({'in': 1})
    assert state['status'] == 'waiting' and state['questions'][0]['request'] == 2
    run.answer(20)
    state = Run(tmp_path / 'run').resume()
    assert state['status'] == 'done' and state['outputs'] == {'out': [41]} and state['rounds'] == {'gate': 1, 'after': 1}
