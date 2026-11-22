"""A graph written out as data builds back into the same graph, and a catalog limits what data may reach."""
import json
import sys

import pytest

from interlacing.base import Firing, GraphBuilder, Port
from interlacing.engine import Engine
from interlacing.methods import Chain, Fan, Governed, Loop
from interlacing.models import ChatResult, FakeProvider
from interlacing.nodes import (AgentNode, Branch, FunctionNode, GraphNode, HumanNode, ProcessNode, SinkNode, SourceNode,
                                 Tool)
from interlacing.protocols import DEFAULT
from interlacing.spec import Catalog, SpecError, dump, load, read, write

HERE = __name__                     # the functions below are found by name in this module


def add_one(x):
    return x + 1


def double(x):
    return x * 2


def big(x):
    return x >= 10


def numbers():
    return iter([1, 2, 3])


WRITTEN = []


def keep(x):
    WRITTEN.append(x)
    return len(WRITTEN)


def hello(request):
    return {'approved': True}


def lead(ctx, task=None, reply=None):
    if task is not None:
        ctx.emit('send', task)
    elif ctx.call('approve', {'result': reply})['approved']:
        ctx.emit('out', reply)


def scripted(name):
    provider = FakeProvider(lambda system, messages, tools: ChatResult(text=f"{name}: {messages[-1]['content']}"))
    provider.name = name
    return provider


def fn(id, function, inputs=('x',), outputs=('y',), **options):
    ports = lambda names: [p if isinstance(p, Port) else Port(p) for p in names]   # noqa: E731
    return FunctionNode(id, function, inputs=ports(inputs), outputs=ports(outputs), **options)


def catalog(**options):
    return Catalog(**{'allow': [HERE], 'processes': True, 'models': scripted, **options})


def everything():
    """One graph with every kind of node that comes with the package."""
    inner = GraphNode('refine', Loop(fn('inc', add_one, inputs=[Port('x', int)], outputs=[Port('y', int)]), until=big, max_rounds=20), DEFAULT)
    program = ProcessNode('echo', [sys.executable, '-c', 'import sys; sys.stdout.write(sys.stdin.read())'], format='json', timeout=20)
    agent = AgentNode('agent', scripted('m1'), system='be brief', tools=[Tool('ask', 'ask a person', {'type': 'object', 'properties': {'q': {}}})])
    builder = GraphBuilder().add(SourceNode('source', numbers, schema=int), inner, program, Branch('gate', big), agent,
                                 HumanNode('person', hello), SinkNode('sink', keep, answers=True))
    builder.connect('source.out', 'refine.in', 'value').connect('refine.out', 'echo.x', 'value').connect('echo.out', 'gate.x', 'value')
    builder.connect('gate.yes', 'agent.task', 'value').connect('agent.ask', 'person.request', 'call').connect('agent.out', 'sink.x', 'latest')
    return builder.output('out', 'sink.out').output('small', 'gate.no').compile(DEFAULT)


def test_a_graph_written_out_builds_back_into_the_same_graph(tmp_path):
    graph = everything()
    data = dump(graph)
    assert json.loads(json.dumps(data)) == data and data['digest'] == graph.digest and len(data['graphs']) == 1
    again = load(data, catalog())
    assert again.digest == graph.digest and again is not graph
    write(tmp_path / 'graph.json', graph)
    assert read(tmp_path / 'graph.json', catalog()).digest == graph.digest
    WRITTEN.clear()
    result = Engine(again, DEFAULT).run()
    assert WRITTEN == ['m1: 10', 'm1: 10', 'm1: 10'] and result.outputs == {'out': [1, 2, 3], 'small': []}


def test_methods_and_coordination_survive_the_round_trip():
    hub = fn('lead', lead, inputs=['task', Port('reply', required=False)], outputs=['out', 'send', Port('approve', call=True)],
             firing=Firing.ANY, context=True)
    graph = Governed(hub, fn('a', add_one), operator=HumanNode('operator'), max_rounds=2).build(DEFAULT)
    asked = []
    again = load(dump(graph), catalog(ask=lambda node, round, request: (asked.append((node, round, request)), {'approved': True})[1]))
    assert again.digest == graph.digest
    assert Engine(again, DEFAULT).run({'in': 1}).outputs == {'out': [2]} and asked == [('operator', 1, {'result': 2})]
    with pytest.raises(Exception, match='nobody is there to ask'):
        Engine(load(dump(graph), catalog()), DEFAULT).run({'in': 1})
    fan = Fan(fn('a', add_one), fn('b', double)).build(DEFAULT)
    assert Engine(load(dump(fan), catalog()), DEFAULT).run({'in': 3}).outputs == {'out': [[4, 6]]}


def test_a_description_can_be_written_by_hand_in_a_shorter_form():
    node = lambda id, function: {'id': id, 'kind': 'function', 'function': f'{HERE}.{function}', 'inputs': ['x'], 'outputs': ['y']}   # noqa: E731
    chain = load({'method': 'chain', 'nodes': [node('a', 'add_one'), node('b', 'double')]}, catalog())
    assert chain.digest == Chain(fn('a', add_one), fn('b', double)).build(DEFAULT).digest
    loop = load({'method': 'loop', 'nodes': [node('a', 'double')], 'until': f'{HERE}.big', 'max_rounds': 9}, catalog())
    assert Engine(loop, DEFAULT).run({'in': 1}).outputs == {'out': [16]}
    plain = load({'nodes': [node('a', 'add_one'), node('b', 'double'), {'id': 'p', 'kind': 'process', 'command': [sys.executable, '-c', 'print(7)'], 'inputs': []}],
                  'edges': [{'from': 'a.y', 'to': 'b.x'}], 'inputs': {'in': 'a.x'}, 'outputs': {'out': 'b.y', 'seven': 'p.out'}}, catalog())
    assert Engine(plain, DEFAULT).run({'in': 1}).outputs == {'out': [4], 'seven': ['7\n']}
    tree = load({'method': 'star', 'max_rounds': 1, 'root': {
        'id': 'lead', 'kind': 'agent', 'model': 'm', 'firing': 'any', 'inputs': ['task', {'name': 'reply', 'required': False}], 'outputs': ['out', 'send']},
        'children': [node('a', 'add_one')]}, catalog())
    assert [n.id for n in tree.nodes] == ['lead', 'a'] and tree.node('lead').firing is Firing.ANY
    typed = load({'nodes': [{'id': 'a', 'kind': 'pass', 'inputs': [{'name': 'x', 'schema': {'record': {'n': 'int', 'tags': {'list': 'str'}}, 'optional': ['tags']}}]}],
                  'inputs': {'in': 'a.x'}, 'outputs': {'out': 'a.y'}})
    assert Engine(typed, DEFAULT).run({'in': {'n': 1}}).outputs == {'out': [{'n': 1}]}


def test_a_catalog_decides_what_a_description_may_reach():
    node = {'id': 'a', 'kind': 'function', 'function': f'{HERE}.add_one', 'inputs': ['x'], 'outputs': ['y']}
    wrap = lambda *nodes: {'nodes': list(nodes)}   # noqa: E731
    with pytest.raises(SpecError, match='a: .*add_one is not in a module this catalog allows \\(none\\)'):
        load(wrap(node))
    with pytest.raises(SpecError, match='os.system is not in a module this catalog allows'):
        load(wrap({**node, 'function': 'os.system'}), catalog())
    with pytest.raises(SpecError, match='p starts a program, and this catalog does not allow processes'):
        load(wrap({'id': 'p', 'kind': 'process', 'command': ['ls']}), Catalog())
    with pytest.raises(SpecError, match="g: this catalog has no models, so the agent's model 'x:y' cannot be built"):
        load(wrap({'id': 'g', 'kind': 'agent', 'model': 'x:y'}), Catalog())
    for name, wrong in ((f'{HERE}.missing', 'cannot be found'), (f'{HERE}.HERE', 'is not callable'), (f'{HERE}.<lambda>', 'top of a module')):
        with pytest.raises(SpecError, match=wrong):
            load(wrap({**node, 'function': name}), catalog())


def test_a_description_that_is_wrong_says_where():
    node = {'id': 'a', 'kind': 'function', 'function': f'{HERE}.add_one', 'inputs': ['x'], 'outputs': ['y']}
    cases = [({'nodes': [{'id': 'a', 'kind': 'teleport'}]}, "unknown kind of node: 'teleport'"),
             ({'nodes': [{'kind': 'function'}]}, 'a node needs an id and a kind'),
             ({'nodes': [{'id': 'a', 'kind': 'function', 'inputs': ['x']}]}, "a: a function node needs 'function'"),
             ({'nodes': [node], 'edges': [{'from': 'a.y'}]}, 'an edge is written'),
             ({'nodes': [node], 'edges': [{'from': 'a.y', 'to': 'b.x'}]}, "no node 'b'"),
             ({'nodes': [{**node, 'inputs': [{'name': 'x', 'schema': 'colour'}]}]}, "a: unknown type in a schema: 'colour'"),
             ({'method': 'spiral', 'nodes': [node]}, "unknown method: 'spiral'"),
             ({'method': 'loop', 'nodes': [node]}, "lacks 'max_rounds'"),
             ({'nodes': [{'id': 'g', 'kind': 'graph', 'graph': 'f' * 64}]}, 'the description has no graph ffffffffffff'),
             ([], 'a description is a mapping')]
    for description, message in cases:
        with pytest.raises(SpecError, match=message):
            load(description, catalog())


def test_a_host_can_add_its_own_kind_of_node(tmp_path):
    from interlacing.nodes import Pass

    class Stamp(Pass):
        kind = 'stamp'

        def config(self):
            return {'mark': self.mark}

        def run(self, ctx, inputs):
            return {'y': f"{inputs['x']}{self.mark}"}

    def stamp(description, catalog, graphs):
        node = Stamp(description['id'])
        node.mark = description['config']['mark']
        return node
    mine = catalog()
    mine.register('stamp', stamp)
    graph = load({'method': 'chain', 'nodes': [{'id': 's', 'kind': 'stamp', 'mark': '!'}]}, mine)
    assert Engine(graph, DEFAULT).run({'in': 'hi'}).outputs == {'out': ['hi!']}
    assert load(dump(graph), mine).digest == graph.digest
    (tmp_path / 'bad.json').write_text('{not json')
    with pytest.raises(SpecError, match='not JSON'):
        read(tmp_path / 'bad.json')


borrowed = __import__("os").getcwd     # a function from elsewhere, here under a name of this module
from interlacing import node as _node_decorator   # noqa: E402


@_node_decorator
def decorated(x: int) -> int:
    return x + 2


def test_a_catalog_reaches_only_what_its_modules_define():
    catalog = Catalog(allow=[HERE])
    assert catalog.function(f'{HERE}.add_one') is add_one
    assert catalog.function(f'{HERE}.decorated') is decorated          # made into a node here, defined here
    for reaching, why in [(f'{HERE}.json.loads', 'reaches into the module json'),
                          (f'{HERE}.sys.exit', 'reaches into the module sys'),
                          (f'{HERE}.borrowed', 'is defined in'),
                          (f'{HERE}.__loader__.load_module', 'not a function a graph may name')]:
        with pytest.raises(SpecError, match=why):
            catalog.function(reaching)


def test_a_graph_written_at_run_time_cannot_reach_through_an_allowed_module():
    from interlacing import run
    from interlacing.spec import SpawnNode
    grow = SpawnNode('grow', Catalog(allow=[HERE]))
    request = {'graph': {'nodes': [{'id': 'x', 'kind': 'function', 'function': f'{HERE}.sys.exit',
                                    'inputs': ['code'], 'outputs': ['out']}],
                         'inputs': {'c': 'x.code'}, 'outputs': {'o': 'x.out'}}, 'inputs': {'c': 3}}
    answer = run(grow, {'request': request}).last('result')
    assert 'reaches into the module sys' in answer['error']


def words(text: str) -> int:
    return len(text.split())


def shout(text: str) -> str:
    return text.upper()


def both(x: list) -> str:
    return f'{x[0]} {x[1]}'


def counted(ctx, text: str) -> int:
    ctx.emit('out', len(text))


def test_a_function_node_reads_its_ports_from_the_function_as_in_python():
    graph = load({'method': 'chain', 'nodes': [{'id': 'w', 'kind': 'function', 'function': f'{HERE}.words'}]},
                 Catalog(allow=[HERE]))
    node = graph.node('w')
    assert list(node.inputs) == ['text'] and list(node.outputs) == ['out'] and not node.context
    assert Engine(graph).run({'in': 'a b c'}).outputs == {'out': [3]}
    with_ctx = load({'nodes': [{'id': 'c', 'kind': 'function', 'function': f'{HERE}.counted'}],
                     'inputs': {'t': 'c.text'}, 'outputs': {'n': 'c.out'}}, Catalog(allow=[HERE]))
    assert with_ctx.node('c').context and Engine(with_ctx).run({'t': 'four'}).outputs == {'n': [4]}
    again = load(dump(graph), Catalog(allow=[HERE]))                 # what is written out builds the same graph
    assert again.digest == graph.digest


def test_a_method_may_be_written_in_place_among_the_nodes_of_another():
    described = {'method': 'chain', 'nodes': [
        {'id': 'up', 'kind': 'function', 'function': f'{HERE}.shout'},
        {'method': 'fan', 'nodes': [{'id': 'w', 'kind': 'function', 'function': f'{HERE}.words'},
                                    {'id': 's', 'kind': 'function', 'function': f'{HERE}.shout'}]},
        {'id': 'pair', 'method': 'chain', 'nodes': [{'id': 'b', 'kind': 'function', 'function': f'{HERE}.both'}]}]}
    graph = load(described, Catalog(allow=[HERE]))
    assert [node.id for node in graph.nodes] == ['up', 'fan', 'pair']
    assert Engine(graph).run({'in': 'a b'}).outputs == {'out': ['2 A B']}


def test_parts_written_in_the_wrong_shape_are_refused_with_the_right_one():
    node = {'id': 'a', 'kind': 'function', 'function': f'{HERE}.add_one', 'inputs': ['x'], 'outputs': ['y']}
    cases = [({'nodes': [node], 'edges': [['a.y', 'a.x']]}, 'an edge is written'),
             ({'nodes': [node], 'inputs': ['a.x']}, 'map each name to "node.port"'),
             ({'nodes': 'a'}, 'nodes is a list'),
             ({'nodes': [{**node, 'inputs': 'x'}]}, 'inputs is a list of ports'),
             ({'nodes': [{**node, 'inputs': [3]}]}, 'a port is a name'),
             ({'nodes': [node], 'manages': ['a']}, 'manages maps a manager')]
    for description, message in cases:
        with pytest.raises(SpecError, match=message):
            load(description, Catalog(allow=[HERE]))


def test_a_wrong_port_or_input_is_answered_with_the_ones_there_are():
    from interlacing import Engine
    catalog = Catalog(allow=[__name__])
    with pytest.raises(SpecError, match=r"has no input port 'txt'; its inputs are \['text'\]"):
        load({'nodes': [{'id': 'a', 'kind': 'function', 'function': f'{__name__}.words'},
                        {'id': 'b', 'kind': 'function', 'function': f'{__name__}.words'}],
              'edges': [{'from': 'a.out', 'to': 'b.txt'}]}, catalog)
    graph = load({'method': 'chain', 'nodes': [{'id': 'a', 'kind': 'function', 'function': f'{__name__}.words'}]}, catalog)
    with pytest.raises(Exception, match=r"no input \['text'\]; its inputs are \['in'\]"):
        Engine(graph).run({'text': 'x'})


class _Doubler:
    def __call__(self, x):
        return x * 2


doubler = _Doubler()
borrowed_partial = __import__('functools').partial(__import__('os').getcwd)


def test_an_object_with_call_counts_where_its_class_was_defined():
    catalog = Catalog(allow=[__name__])
    assert catalog.function(f'{__name__}.doubler')(4) == 8
    with pytest.raises(SpecError, match='defined in posix|defined in nt'):
        catalog.function(f'{__name__}.borrowed_partial')
