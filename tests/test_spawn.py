"""A graph that is only decided while the run is going on: written as data, built and run inside one node."""
import json

import pytest

from interlacing.base import GraphBuilder, NodeError, Port
from interlacing.engine import Engine, Journal, RunError
from interlacing.methods import Chain
from interlacing.models import ChatResult, FakeProvider
from interlacing.nodes import AgentNode, FunctionNode, Tool
from interlacing.protocols import DEFAULT
from interlacing.runs import Run
from interlacing.spec import Catalog, SpawnNode, dump, load

HERE = __name__
SEEN = []
BROKEN = []


def add_one(x):
    return x + 1


def double(x):
    return x * 2


def fragile(x):
    SEEN.append(x)
    if BROKEN:
        raise OSError('not yet')
    return x + 100


def node(id, function):
    return {'id': id, 'kind': 'function', 'function': f'{HERE}.{function}', 'inputs': ['x'], 'outputs': ['y']}


def chain(*functions):
    return {'method': 'chain', 'nodes': [node(f'n{i}', name) for i, name in enumerate(functions, 1)]}


def plan(x):
    """Decides, from what it is given, how long a chain it needs."""
    return {'graph': chain(*(['double'] * x['doublings'])), 'inputs': {'in': x['start']}}


def outer(catalog, **options):
    planner = FunctionNode('plan', plan, inputs=[Port('x')], outputs=[Port('y')])
    return Chain(planner, SpawnNode('grow', catalog, **options)).build(DEFAULT)


def test_a_graph_decided_during_the_run_is_built_and_run(tmp_path):
    catalog = Catalog(allow=[HERE])
    seen = []
    result = Engine(outer(catalog), DEFAULT, observe=seen.append).run({'in': {'doublings': 3, 'start': 5}})
    assert result.last('out') == {'outputs': {'out': [40]}, 'rounds': {'n1': 1, 'n2': 1, 'n3': 1}, 'problems': []}
    assert result.rounds == {'plan': 1, 'grow': 1}                  # the outer graph did not change
    assert [e['node'] for e in seen if e.get('within') == ['grow'] and e['event'] == 'started'] == ['n1', 'n2', 'n3']
    assert Engine(outer(catalog), DEFAULT).run({'in': {'doublings': 1, 'start': 5}}).last('out')['outputs'] == {'out': [10]}


def test_what_a_spawned_graph_may_reach_is_the_catalogs_to_say():
    spawn = SpawnNode('grow', Catalog(allow=[HERE]), max_nodes=3)
    ctx = type('Ctx', (), {'round': 1, 'problems': (), 'stopping': lambda self: False})()
    ask = lambda graph, **more: spawn.run(ctx, {'request': {'graph': graph, **more}})['result']['error']   # noqa: E731
    assert 'os.system is not in a module this catalog allows' in ask({'method': 'chain', 'nodes': [
        {'id': 'a', 'kind': 'function', 'function': 'os.system', 'inputs': ['x'], 'outputs': ['y']}]})
    assert 'does not allow processes' in ask({'nodes': [{'id': 'p', 'kind': 'process', 'command': ['ls']}]})
    assert 'no models' in ask({'method': 'chain', 'nodes': [{'id': 'a', 'kind': 'agent', 'model': 'x'}]})
    assert 'the graph has 4 nodes, and at most 3 may be made here' in ask(chain('double', 'double', 'double', 'double'))
    assert 'at most 3' in ask({'method': 'chain', 'nodes': [{'id': 'b', 'kind': 'block', 'graph': chain('double', 'double', 'double')}]})
    assert 'may not make graphs of its own' in ask({'nodes': [{'id': 's', 'kind': 'spawn'}]})
    assert "the graph has no input ['x']; its inputs are ['in']" in ask(chain('double'), inputs={'x': 1})
    assert 'graph must be the description of a graph' in ask('a chain please')
    assert 'inputs must be a mapping' in ask(chain('double'), inputs=[1])
    for wrong in ({'max_nodes': 0}, {'depth': -1}, {'max_runs': 0}, {'workers': -1}):
        with pytest.raises(NodeError):
            SpawnNode('grow', Catalog(), **wrong)
    with pytest.raises(NodeError, match='needs the catalog'):
        SpawnNode('grow', None)


def test_a_graph_that_fails_is_answered_with_the_error_unless_the_node_is_strict():
    catalog = Catalog(allow=[HERE])
    BROKEN[:] = [True]
    answer = Engine(outer(catalog), DEFAULT).run({'in': {'doublings': 0, 'start': 1}}).last('out')
    assert 'the graph cannot be built: a chain needs at least one node' in answer['error']
    broken = FunctionNode('plan', lambda x: {'graph': chain('fragile'), 'inputs': {'in': 1}}, inputs=[Port('x')], outputs=[Port('y')])
    answer = Engine(Chain(broken, SpawnNode('grow', catalog)).build(DEFAULT), DEFAULT).run({'in': 0}).last('out')
    assert answer == {'error': 'node n1 failed in round 1: not yet', 'node': 'n1'}
    with pytest.raises(RunError, match='node grow failed in round 1: node n1 failed in round 1: not yet'):
        Engine(Chain(broken, SpawnNode('grow', catalog, strict=True)).build(DEFAULT), DEFAULT).run({'in': 0})
    with pytest.raises(RunError, match='node grow failed in round 1: the graph cannot be built'):
        Engine(outer(catalog, strict=True), DEFAULT).run({'in': {'doublings': 0, 'start': 1}})
    BROKEN.clear()


def test_a_spawned_graph_is_taken_up_after_a_restart_and_the_number_of_graphs_is_limited(tmp_path):
    catalog = Catalog(allow=[HERE])
    request = FunctionNode('plan', lambda x: {'graph': chain('fragile', 'fragile'), 'inputs': {'in': x}}, inputs=[Port('x')], outputs=[Port('y')])
    build = lambda: Chain(request, SpawnNode('grow', catalog, strict=True)).build(DEFAULT)   # noqa: E731
    SEEN.clear()
    BROKEN.clear()

    def break_after_first(event):
        if event['event'] == 'finished' and event.get('within') == ['grow']:
            BROKEN.append(True)
    with pytest.raises(RunError, match='node n2 failed'):
        Engine(build(), DEFAULT, journal=Journal(tmp_path / 'j'), observe=break_after_first).run({'in': 1})
    BROKEN.clear()
    result = Engine(build(), DEFAULT, journal=Journal(tmp_path / 'j')).resume()
    assert result.last('out')['outputs'] == {'out': [201]} and SEEN == [1, 101, 101]      # n1 did not run again

    twice = (GraphBuilder().add(FunctionNode('plan', lambda ctx, x: [ctx.emit('y', {'graph': chain('double'), 'inputs': {'in': n}}) for n in x] and None,
                                             context=True, inputs=[Port('x')], outputs=[Port('y')]), SpawnNode('grow', catalog, max_runs=2))
             .connect('plan.y', 'grow.request', 'value').input('in', 'plan.x').output('out', 'grow.result').compile(DEFAULT))
    out = Engine(twice, DEFAULT).run({'in': [1, 2, 3]}).outputs['out']
    assert [o.get('outputs') for o in out[:2]] == [{'out': [2]}, {'out': [4]}] and 'that is the limit' in out[2]['error']


def test_an_agent_grows_a_graph_with_a_tool_and_levels_are_limited():
    catalog = Catalog(allow=[HERE])
    asked = []

    def policy(system, messages, tools):
        asked.append(messages[-1])
        if len(asked) == 1:
            return ChatResult(tool_calls=[{'id': 'c1', 'name': 'grow', 'args': {'graph': chain('nothing'), 'inputs': {'in': 4}}}])
        if len(asked) == 2:
            return ChatResult(tool_calls=[{'id': 'c2', 'name': 'grow', 'args': {'graph': chain('double', 'add_one'), 'inputs': {'in': 4}}}])
        return ChatResult(text=str(json.loads(messages[-1]['content'])['outputs']['out'][0]))
    agent = AgentNode('lead', FakeProvider(policy), tools=[Tool('grow', 'Run a graph given as data.')])
    graph = (GraphBuilder().add(agent, SpawnNode('grow', catalog)).connect('lead.grow', 'grow.request', 'call')
             .input('in', 'lead.task').output('out', 'lead.out').compile(DEFAULT))
    assert Engine(graph, DEFAULT).run({'in': 'compute'}).last('out') == '9'
    assert 'cannot be found' in asked[1]['content']                 # the model was told its first graph could not be built

    nested = {'nodes': [{'id': 'inner', 'kind': 'spawn'}], 'inputs': {'in': 'inner.request'}, 'outputs': {'out': 'inner.result'}}
    request = {'graph': nested, 'inputs': {'in': {'graph': chain('double'), 'inputs': {'in': 3}}}}
    two_levels = Chain(FunctionNode('plan', lambda x: request, inputs=[Port('x')], outputs=[Port('y')]), SpawnNode('grow', catalog, depth=1)).build(DEFAULT)
    assert Engine(two_levels, DEFAULT).run({'in': 0}).last('out')['outputs']['out'][0]['outputs'] == {'out': [6]}
    deeper = {'graph': nested, 'inputs': {'in': request}}
    three = Chain(FunctionNode('plan', lambda x: deeper, inputs=[Port('x')], outputs=[Port('y')]), SpawnNode('grow', catalog, depth=1)).build(DEFAULT)
    assert 'may not make graphs of its own' in Engine(three, DEFAULT).run({'in': 0}).last('out')['outputs']['out'][0]['error']


def test_a_spawn_node_is_written_as_data_and_people_in_grown_graphs_are_told_apart(tmp_path):
    description = {'method': 'chain', 'nodes': [node('plan', 'ask_twice'), {'id': 'grow', 'kind': 'spawn', 'max_nodes': 5}]}
    catalog = Catalog(allow=[HERE])
    graph = load(description, catalog)
    assert graph.node('grow').max_nodes == 5 and load(dump(graph), catalog).digest == graph.digest
    run = Run.create(tmp_path / 'run', description, allow=[HERE])
    state = run.start({'in': 3})
    assert state['status'] == 'waiting' and [q['id'] for q in state['questions']] == ['grow.1__first-1']
    run.answer(10)
    state = Run(tmp_path / 'run').resume()
    assert [q['id'] for q in state['questions']] == ['grow.1__second-1'] and state['questions'][0]['request'] == 10
    Run(tmp_path / 'run').answer(7)
    state = Run(tmp_path / 'run').resume()
    assert state['status'] == 'done' and state['outputs']['out'][0]['outputs'] == {'out': [7]}


def ask_twice(x):
    return {'graph': {'method': 'chain', 'nodes': [{'id': 'first', 'kind': 'human'}, {'id': 'second', 'kind': 'human'}]}, 'inputs': {'in': x}}


def slow(x):
    import time
    time.sleep(0.4)
    return x


def test_a_graph_that_is_not_trusted_cannot_give_itself_more_time_or_threads(tmp_path):
    patient = {'id': 'n1', 'kind': 'function', 'function': f'{HERE}.slow', 'inputs': ['x'], 'outputs': ['y'], 'time_limit': 60}
    request = FunctionNode('plan', lambda x: {'graph': {'method': 'chain', 'nodes': [patient]}, 'inputs': {'in': x}}, inputs=[Port('x')], outputs=[Port('y')])
    grown = Chain(request, SpawnNode('grow', Catalog(allow=[HERE]))).build(DEFAULT)
    assert 'node n1 ran longer than 0.1 seconds' in Engine(grown, DEFAULT, time_limit=0.1).run({'in': 1}).last('out')['error']
    written = FunctionNode('n1', slow, inputs=[Port('x')], outputs=[Port('y')])
    written.time_limit = 60                                     # written in Python, by whoever runs it: it stands
    assert Engine(Chain(written).build(DEFAULT), DEFAULT, time_limit=0.1).run({'in': 1}).last('out') == 1
    with pytest.raises(RunError, match='node n1 ran longer than 0.1 seconds'):
        Engine(Chain(written).build(DEFAULT), DEFAULT, time_limit=0.1, firm=True).run({'in': 1})

    description = {'method': 'chain', 'nodes': [patient]}
    assert Run.create(tmp_path / 'own', description, allow=[HERE], time_limit=0.1).start({'in': 1})['status'] == 'done'
    state = Run.create(tmp_path / 'theirs', description, allow=[HERE], time_limit=0.1, strict=True).start({'in': 1})
    assert state['status'] == 'failed' and 'ran longer than 0.1 seconds' in state['error']

    spawn = SpawnNode('grow', Catalog(allow=[HERE]), max_nodes=6, depth=1, workers=2)
    built = spawn.build({'nodes': [{'id': 'deep', 'kind': 'spawn', 'max_nodes': 10_000, 'workers': 500, 'depth': 9},
                                   {'id': 'b', 'kind': 'block', 'workers': 500, 'graph': chain('double')}],
                         'inputs': {'ask': 'deep.request', 'x': 'b.in'}})
    deep, block = built.node('deep'), built.node('b')
    assert (deep.max_nodes, deep.workers, deep.depth, block.workers) == (6, 2, 0, 2)


def test_a_spawn_node_as_a_tool_carries_the_format_and_what_it_may_name():
    from interlacing.spec import GUIDE
    grow = SpawnNode('grow', Catalog(allow=[HERE]), max_nodes=7)
    tool = grow.tool()
    assert tool.name == 'grow' and GUIDE in tool.description and 'At most 7 nodes' in tool.description
    assert f'{HERE}.add_one(x)' in tool.description and f'{HERE}.node(' not in tool.description.replace(f'{HERE}.node(id', '')
    assert tool.parameters['required'] == ['graph', 'inputs']

    def policy(system, messages, tools):
        if not any(m['role'] == 'tool' for m in messages):
            return ChatResult(tool_calls=[{'id': 'c1', 'name': 'grow', 'args': {
                'graph': {'method': 'chain', 'nodes': [{'id': 'a', 'kind': 'function', 'function': f'{HERE}.add_one'},
                                                       {'id': 'b', 'kind': 'function', 'function': f'{HERE}.double'}]},
                'inputs': {'in': 4}}}])
        return ChatResult(text=messages[-1]['content'])
    agent = AgentNode('designer', FakeProvider(policy), tools=[tool])
    graph = (GraphBuilder().add(agent, grow).connect('designer.grow', 'grow.request', 'call')
             .input('task', 'designer.task').output('out', 'designer.out').compile())
    assert json.loads(Engine(graph).run({'task': 'go'}).last('out'))['outputs'] == {'out': [10]}


def test_the_guide_lists_only_what_the_catalog_lets_a_graph_name():
    from interlacing.spec import functions, guide
    listed = {f['name']: f for f in functions(Catalog(allow=[HERE]))}
    assert f'{HERE}.add_one' in listed and listed[f'{HERE}.add_one']['signature'] == '(x)'
    assert not any(name.endswith(('.json', '.pytest', '.Catalog')) for name in listed)     # imported, not defined here
    assert 'Allowed functions: none.' in guide(Catalog()) and 'Allowed functions: none.' in guide()
    assert functions(Catalog(allow=['no_such_module_anywhere'])) == []
