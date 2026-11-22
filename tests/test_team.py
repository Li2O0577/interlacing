"""Jurisdiction: a node gains ports and members while the run goes on, manages them, and an agent may hire."""
import threading
import time

import pytest

from interlacing import AgentNode, Engine, FunctionNode, GraphBuilder, GraphNode, Journal, Port
from interlacing.base import Change, GraphError
from interlacing.models import ChatResult, FakeProvider
from interlacing.spec import Catalog, dump, load


def helper(id):
    return FunctionNode(id, lambda task: f"{task}: done", inputs=[Port('task')], outputs=[Port('out')])


# ---- the engine ------------------------------------------------------------------------------------------

def grower(seen):
    def run(ctx, x):
        ctx.change(Change().add(helper('g-h')).ports('g', inputs=['back'], outputs=['work'])
                   .connect('g.work', 'g-h.task').connect('g-h.out', 'g.back', back=True, max_rounds=2)
                   .manage('g', 'g-h'))
        seen.append(ctx.team())
        ctx.emit('work', x)
        got = ctx.receive('back')
        return got['back']
    return FunctionNode('g', run, inputs=[Port('x')], outputs=[Port('out')], context=True)


@pytest.mark.parametrize('workers', [0, 1, 4])
def test_a_node_gets_ports_and_a_member_it_manages_while_it_runs(workers):
    seen = []
    node = grower(seen)
    graph = GraphBuilder().add(node).input('in', 'g.x').output('out', 'g.out').compile()
    engine = Engine(graph, workers=workers)
    result = engine.run({'in': 'sum'})
    assert result.last('out') == 'sum: done'
    assert seen[0] == {'g-h': {'status': 'active', 'running': False, 'waiting': False, 'manager': 'g', 'rounds': 0}}
    assert engine.graph.managers == {'g-h': 'g'} and set(node.inputs) == {'x', 'back'}


def test_ports_given_by_a_change_that_is_refused_are_taken_back():
    node = FunctionNode('g', lambda x: x, inputs=[Port('x')], outputs=[Port('out')])
    graph = GraphBuilder().add(node).input('in', 'g.x').compile()

    def run(ctx, x):
        try:
            ctx.change(Change().ports('g', outputs=['extra']).connect('g.extra', 'nobody.x'))
        except GraphError as exc:
            return str(exc)
    node = FunctionNode('g', run, inputs=[Port('x')], outputs=[Port('out')], context=True)
    graph = GraphBuilder().add(node).input('in', 'g.x').output('out', 'g.out').compile()
    assert 'nobody' in Engine(graph).run({'in': 1}).last('out') and set(node.outputs) == {'out'}
    with pytest.raises(GraphError, match='takes any value'):
        Change().ports('g', outputs=[Port('typed', int)])


def test_a_change_with_ports_and_managers_is_made_again_when_the_run_is_taken_up(tmp_path):
    path = tmp_path / 'j.jsonl'
    graph = lambda: GraphBuilder().add(grower([])).input('in', 'g.x').output('out', 'g.out').compile()   # noqa: E731
    first = Engine(graph(), journal=Journal(path), rebuild=lambda d: helper(d['id']))
    assert first.run({'in': 'a'}).last('out') == 'a: done'
    again = Engine(graph(), journal=Journal(path), rebuild=lambda d: helper(d['id']))
    assert again.resume().last('out') == 'a: done'
    assert again.graph.managers == {'g-h': 'g'} and 'back' in again.graph.node('g').inputs


def test_a_node_of_a_graph_run_inside_a_node_is_switched_by_its_path():
    started = threading.Event()

    def slow(ctx, x):
        started.set()
        while ctx.checkpoint():
            time.sleep(0.01)
        return None
    inner = GraphBuilder().add(FunctionNode('slow', slow, inputs=[Port('x')], outputs=[Port('y')], context=True)) \
        .input('x', 'slow.x').output('y', 'slow.y').compile()
    box = GraphNode('box', inner)
    graph = GraphBuilder().add(box).input('in', 'box.x').output('out', 'box.y').compile()
    engine = Engine(graph, workers=1)
    done = {}
    thread = threading.Thread(target=lambda: done.setdefault('result', engine.run({'in': 1})))
    thread.start()
    assert started.wait(5)
    assert engine.control('box/slow', 'stop') == ['box/slow']
    with pytest.raises(Exception, match='no node'):
        engine.control('nobody/slow', 'stop')
    thread.join(10)
    assert not thread.is_alive()


# ---- an agent that hires ---------------------------------------------------------------------------------

def call(tool, **args):
    return {'id': f'c-{tool}-{len(args)}', 'name': tool, 'args': args}


def told(messages):
    return next((m['content'] for m in reversed(messages) if m['role'] == 'tool'), '')


def lead_policy(said_log):
    def policy(system, messages, tools):
        said = told(messages)
        said_log.append((said, [t['name'] for t in tools]))
        if not said:
            return ChatResult(tool_calls=[call('hire', template='helper', name='h1', task='count'),
                                          call('hire', template='helper', name='h2'), call('wait')])
        if '[from_h1]' in said:
            return ChatResult(tool_calls=[call('team'), call('send_out', content=said.split('\n')[1]),
                                          call('dismiss', name='h1'), call('finish')])
        return ChatResult(tool_calls=[call('finish')])
    return policy


@pytest.mark.parametrize('workers', [0, 1, 2])
def test_an_agent_hires_a_member_gives_it_work_hears_back_and_dismisses_it(workers):
    log = []
    lead = AgentNode('lead', FakeProvider(lead_policy(log)), resident=True, hire=1, templates={'helper': helper})
    graph = GraphBuilder().add(lead).input('in', 'lead.task').output('out', 'lead.out').compile()
    engine = Engine(graph, workers=workers)
    result = engine.run({'in': 'go'})
    assert result.last('out') == 'count: done' and result.complete
    answers = [m['content'] for m in lead.messages if m['role'] == 'tool']
    assert answers[0].startswith('hired h1: send it work with send_to_h1') and 'You may hire 0 more' in answers[0]
    assert answers[1] == 'error: you have hired all you may (1)'
    assert any(a.startswith('Your team:\n- h1:') for a in answers) and 'dismissed h1' in answers
    assert 'lead-h1' not in {n.id for n in engine.graph.nodes} and lead.hires == 1 and lead.hired == {}
    assert {'hire', 'dismiss', 'control', 'team', 'connect', 'send_to_h1'} <= set(log[-1][1])


def test_hiring_is_off_unless_it_is_given():
    plain = AgentNode('a', FakeProvider(lambda s, m, t: ChatResult(text='x')), resident=True)
    assert not {'hire', 'dismiss', 'team'} & {spec['name'] for spec in plain._specs}
    with pytest.raises(Exception, match='only a resident agent may hire'):
        AgentNode('a', FakeProvider(lambda s, m, t: ChatResult(text='x')), hire=2, templates={'h': helper})
    with pytest.raises(Exception, match='needs templates'):
        AgentNode('a', FakeProvider(lambda s, m, t: ChatResult(text='x')), resident=True, hire=2)
    with pytest.raises(Exception, match='whole number'):
        AgentNode('a', FakeProvider(lambda s, m, t: ChatResult(text='x')), resident=True, hire=True, templates={'h': helper})


def test_a_member_may_hire_no_more_than_who_hired_it():
    def member(id):
        return AgentNode(id, FakeProvider(lambda s, m, t: ChatResult(tool_calls=[call('finish')])), resident=True,
                         hire=5, templates={'helper': helper})
    made = []

    def policy(system, messages, tools):
        if not told(messages):
            return ChatResult(tool_calls=[call('hire', template='sub', name='s'), call('finish')])
        return ChatResult(tool_calls=[call('finish')])

    def sub(id):
        made.append(member(id))
        return made[-1]
    lead = AgentNode('lead', FakeProvider(policy), resident=True, hire=2, templates={'sub': sub})
    Engine(GraphBuilder().add(lead).input('in', 'lead.task').compile()).run({'in': 'go'})
    assert made[0].hire == 2


def test_hiring_from_a_graph_file_with_the_catalog_templates(tmp_path):
    def models(name):
        if name == 'lead':
            return FakeProvider(lead_policy([]))
        return FakeProvider(lambda s, m, t: ChatResult(text=f"{m[-1]['content']}: done"))
    catalog = Catalog(models=models, templates={'helper': {'kind': 'agent', 'model': 'member'}},
                      workspace=tmp_path)
    described = {'nodes': [{'id': 'lead', 'kind': 'agent', 'model': 'lead', 'resident': True, 'hire': 1,
                            'templates': ['helper']}],
                 'inputs': {'in': 'lead.task'}, 'outputs': {'out': 'lead.out'}}
    graph = load(described, catalog)
    assert dump(graph)['nodes'][0]['config']['templates'] == {'helper': {'kind': 'agent', 'model': 'member'}}
    engine = Engine(graph, protocols=catalog.protocols)
    result = engine.run({'in': 'go'})
    assert result.last('out') == 'count: done'
    with pytest.raises(Exception, match='no template'):
        load({**described, 'nodes': [{**described['nodes'][0], 'templates': ['nobody']}]}, catalog)


def test_a_member_works_in_a_directory_inside_that_of_who_hired_it(tmp_path):
    catalog = Catalog(models=lambda name: FakeProvider(lambda s, m, t: ChatResult(text='x')),
                      templates={'helper': {'kind': 'agent', 'model': 'm'}}, workspace=tmp_path)
    graph = load({'nodes': [{'id': 'lead', 'kind': 'agent', 'model': 'm', 'resident': True, 'hire': 1,
                             'templates': ['helper']}], 'inputs': {'in': 'lead.task'}}, catalog)
    lead = graph.node('lead')
    member = lead._member(lead.templates['helper'], 'lead-h1')
    assert member.area.home == tmp_path / 'lead' / 'lead-h1'
    assert member.config()['layer'] == ['lead']          # so that it is made the same way when the run is taken up


@pytest.mark.parametrize('workers', [0, 1, 2])
def test_a_member_that_is_still_working_when_dismissed_is_taken_out_once_it_has_stopped(workers):
    def member(id):
        def policy(system, messages, tools):
            if not told(messages):
                return ChatResult(tool_calls=[call('send_out', content='hi'), call('wait')])
            return ChatResult(tool_calls=[call('wait')])
        return AgentNode(id, FakeProvider(policy), resident=True)
    answers = []

    def policy(system, messages, tools):
        said = told(messages)
        answers.append(said)
        if not said:
            return ChatResult(tool_calls=[call('hire', template='m', name='a', task='go'), call('wait')])
        if '[from_a]' in said:
            return ChatResult(tool_calls=[call('dismiss', name='a'), call('hire', template='m', name='a'),
                                          call('wait')])
        if said.startswith('Nothing more can come'):
            return ChatResult(tool_calls=[call('team'), call('finish')])
        return ChatResult(tool_calls=[call('finish')])
    lead = AgentNode('lead', FakeProvider(policy), resident=True, hire=2, templates={'m': member})
    engine = Engine(GraphBuilder().add(lead).input('in', 'lead.task').compile(), workers=workers)
    assert engine.run({'in': 'go'}).complete
    answers = [m['content'] for m in lead.messages if m['role'] == 'tool']
    assert any(a.startswith('dismissed a') for a in answers)
    assert any(a.startswith('error: ') and "'a'" in a for a in answers)     # its name is not free until it is out
    assert any(a.startswith('You have no team.') for a in answers) and lead.leaving == {} and lead.hires == 1
    assert {node.id for node in engine.graph.nodes} == {'lead'}


@pytest.mark.parametrize('workers', [0, 1, 2])
def test_a_member_that_fails_is_reported_to_its_hirer_which_restarts_it(workers):
    tries = []

    def shaky(id):
        def run(task):
            tries.append(task)
            if len(tries) == 1:
                raise ValueError('the disk was full')
            return f'{task}: done'
        return FunctionNode(id, run, inputs=[Port('task')], outputs=[Port('out')])
    told = []

    def policy(system, messages, tools):
        said = next((m['content'] for m in reversed(messages) if m['role'] == 'tool'), '')
        if not said:
            return ChatResult(tool_calls=[call('hire', template='s', name='a', task='count'), call('wait')])
        if '[failed]' in said:
            told.append(said)
            return ChatResult(tool_calls=[call('control', name='a', action='restart'), call('wait')])
        if '[from_a]' in said:
            return ChatResult(tool_calls=[call('send_out', content=said.split('\n')[1]), call('finish')])
        return ChatResult(tool_calls=[call('wait')])
    lead = AgentNode('lead', FakeProvider(policy), resident=True, hire=1, templates={'s': shaky})
    graph = GraphBuilder().add(lead).input('in', 'lead.task').output('out', 'lead.out').compile()
    result = Engine(graph, workers=workers).run({'in': 'go'})
    assert result.last('out') == 'count: done' and result.complete and tries == ['count', 'count']
    assert told[0].startswith('[failed] a failed in its round 1: the disk was full')
    assert result.problems[0]['handled'] == 'reported'


def test_a_hirer_may_give_up_when_a_member_fails():
    def broken(id):
        return FunctionNode(id, lambda task: 1 / 0, inputs=[Port('task')], outputs=[Port('out')])

    def policy(system, messages, tools):
        said = next((m['content'] for m in reversed(messages) if m['role'] == 'tool'), '')
        if not said:
            return ChatResult(tool_calls=[call('hire', template='b', name='a', task='x'), call('wait')])
        return ChatResult(tool_calls=[call('fail', reason='a cannot divide')])
    lead = AgentNode('lead', FakeProvider(policy), resident=True, hire=1, templates={'b': broken})
    with pytest.raises(Exception, match='lead gave up: a cannot divide'):
        Engine(GraphBuilder().add(lead).input('in', 'lead.task').compile(), workers=1).run({'in': 'go'})


def test_a_journaled_run_whose_agent_hires_is_taken_up_without_a_rebuild(tmp_path):
    path = tmp_path / 'j.jsonl'

    def graph():
        lead = AgentNode('lead', FakeProvider(lead_policy([])), resident=True, hire=1, templates={'helper': helper})
        return GraphBuilder().add(lead).input('in', 'lead.task').output('out', 'lead.out').compile()
    first = Engine(graph(), journal=Journal(path))
    assert first.run({'in': 'go'}).last('out') == 'count: done'
    again = Engine(graph(), journal=Journal(path))
    assert again.resume().last('out') == 'count: done'


def test_a_hired_member_is_made_again_from_the_template_that_made_it():
    lead = AgentNode('lead', FakeProvider(lead_policy([])), resident=True, hire=1, templates={'helper': helper})
    member = lead.remake('helper', 'lead-h1')
    assert member.id == 'lead-h1' and member.made_by == {'node': 'lead', 'template': 'helper'}
    added = Change().add(member).describe()
    assert added[0][1]['made_by'] == {'node': 'lead', 'template': 'helper'}
