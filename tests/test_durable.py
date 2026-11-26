"""SQLite delivery commits and recovery checkpoints across process failures."""
import json
import sqlite3
import subprocess
import sys
import threading
from contextlib import closing

import pytest

from interlacing import Chain, Engine, FunctionNode, GraphBuilder, GraphError, Port, SQLiteJournal
from interlacing.engine import RunError
from interlacing.inspection import explain_records
from interlacing.protocols import standard
from interlacing.protocols.store import DirectoryStore


def function(id, action, *, context=False):
    return FunctionNode(id, action, inputs=[Port('x')], outputs=[Port('y')], context=context)


def test_a_committed_delivery_reaches_its_receiver_while_sender_and_sibling_are_running(tmp_path):
    journal = SQLiteJournal(tmp_path / 'journal.sqlite3', durable=True, checkpoint_every=2)
    source_mutated, sink_finished = threading.Event(), threading.Event()

    def source(ctx, x):
        value = {'number': x}
        ctx.emit('y', value)
        value['number'] = 999
        source_mutated.set()
        assert sink_finished.wait(5)

    def slow(x):
        assert sink_finished.wait(5)
        return x

    def sink(x):
        assert source_mutated.wait(5)
        delivery = next(rec for rec in journal.read() if rec['kind'] == 'deliver' and rec['node'] == 'source')
        assert delivery['sent'][0][3] == ['v', {'number': 4}]
        assert x == {'number': 4}
        x['number'] = 1000
        sink_finished.set()
        return 8

    graph = (GraphBuilder().add(function('source', source, context=True), function('slow', slow), function('sink', sink))
             .input('number', 'source.x').input('other', 'slow.x').connect('source.y', 'sink.x')
             .output('out', 'sink.y').compile())
    result = Engine(graph, workers=3, journal=journal).run({'number': 4, 'other': 9})
    assert result.complete and result.last('out') == 8
    assert Engine(graph, journal=journal).resume().outputs == result.outputs


CRASH_RUN = """
import json
import os
import sys
import threading
from pathlib import Path
from interlacing import Chain, Engine, FunctionNode, Port, SQLiteJournal

path, flag, phase, *edge = sys.argv[1:]
def producer(ctx, x):
    ctx.emit('y', x + (40 if edge else 1))
    if phase == 'after-consumer':
        assert threading.Event().wait(10)
    return None

def consumer(x):
    with Path(flag).open('a') as file:
        file.write('consumed\\n')
    return x * 2

graph = Chain(FunctionNode('producer', producer, inputs=[Port('x')], outputs=[Port('y')], context=True),
              FunctionNode('consumer', consumer, inputs=[Port('x')], outputs=[Port('y')])).build()
journal = SQLiteJournal(path, durable=True, checkpoint_every=2)
engine = Engine(graph, journal=journal, workers=2)

if phase == 'resume':
    print(json.dumps(engine.resume().outputs))
else:
    write = SQLiteJournal.write
    def cut(self, record):
        hit = ((record['kind'], record.get('node')) ==
               {'before-delivery': ('deliver', 'producer'), 'after-delivery': ('deliver', 'producer'),
                'after-binding': ('bind', 'consumer'), 'after-consumer': ('act', 'consumer')}[phase])
        if hit and phase == 'before-delivery':
            os._exit(37)
        write(self, record)
        if hit:
            os._exit(37)
    SQLiteJournal.write = cut
    engine.run({'in': 3})
"""


@pytest.mark.parametrize('phase', ['before-delivery', 'after-delivery', 'after-binding', 'after-consumer'])
def test_process_recovery_deduplicates_deliveries_with_a_checkpoint_and_tail(tmp_path, phase):
    path, calls = tmp_path / 'run.sqlite3', tmp_path / 'calls.txt'
    command = [sys.executable, '-c', CRASH_RUN, str(path), str(calls)]
    assert subprocess.run([*command, phase], timeout=15).returncode == 37
    expected_calls = ['consumed'] if phase == 'after-consumer' else []
    assert (calls.read_text().splitlines() if calls.exists() else []) == expected_calls
    journal = SQLiteJournal(path, durable=True)
    checkpoint, tail = journal.recovery()
    assert checkpoint and checkpoint['seq'] >= 2
    if phase == 'after-delivery':
        assert tail[-1]['kind'] == 'deliver'
    if phase == 'before-delivery':
        assert all(record['kind'] != 'deliver' for record in journal.read())
    if phase == 'after-consumer':
        assert all((record['kind'], record.get('node')) != ('act', 'producer') for record in journal.read())

    outcome = subprocess.check_output([*command, 'resume'], text=True, timeout=15)
    assert json.loads(outcome) == {'out': [8]}
    records = journal.read()
    assert len([r for r in records if r['kind'] == 'deliver' and r['node'] == 'producer']) == 1
    assert len([r for r in records if r['kind'] == 'bind' and r['node'] == 'consumer']) == 1
    assert calls.read_text().splitlines() == ['consumed']
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("DELETE FROM checkpoints")
    assert json.loads(subprocess.check_output([*command, 'resume'], text=True, timeout=15)) == {'out': [8]}
    assert journal.read() == records
    assert calls.read_text().splitlines() == ['consumed']


def test_replay_rejects_changed_output_instead_of_republishing(tmp_path):
    path, calls = tmp_path / 'run.sqlite3', tmp_path / 'calls.txt'
    command = [sys.executable, '-c', CRASH_RUN, str(path), str(calls)]
    assert subprocess.run([*command, 'after-delivery'], timeout=15).returncode == 37
    resumed = subprocess.run([*command, 'resume', 'different-output'], text=True, capture_output=True, timeout=15)
    assert resumed.returncode != 0 and 'diverged at deliver' in resumed.stderr
    assert sum(record['kind'] == 'deliver' for record in SQLiteJournal(path).read()) == 1


CHANGE_RUN = """
import json
import os
import sys
from interlacing import Change, Engine, FunctionNode, GraphBuilder, Port, SQLiteJournal, dump
from interlacing.inspection import explain_records

def double(x):
    return x * 2

def make(id):
    return FunctionNode(id, double, inputs=[Port('x')], outputs=[Port('y')])

def grow(ctx, x):
    ctx.change(Change().add(make('added')).connect('grow.y', 'added.x').output('result', 'added.y'))
    return x

graph = GraphBuilder().add(FunctionNode('grow', grow, inputs=[Port('x')], outputs=[Port('y')], context=True)).input('in', 'grow.x').compile()
journal = SQLiteJournal(sys.argv[1], durable=True, checkpoint_every=2)
engine = Engine(graph, journal=journal, rebuild=lambda description: make(description['id']))
phase = sys.argv[2]
if phase == 'resume':
    output = engine.resume().outputs
    view = explain_records(dump(graph), journal.inspect(), 'done', [], timeline=True)
    print(json.dumps({'output': output, 'nodes': list(view['nodes'])}))
else:
    write = SQLiteJournal.write
    def cut(self, record):
        if record['kind'] == 'topology' and phase == 'before':
            os._exit(37)
        write(self, record)
        if record['kind'] == 'topology' and phase == 'after':
            os._exit(37)
    SQLiteJournal.write = cut
    engine.run({'in': 4})
"""


@pytest.mark.parametrize('phase', ['before', 'after'])
def test_graph_change_is_recovered_on_either_side_of_its_commit(tmp_path, phase):
    path = tmp_path / 'run.sqlite3'
    command = [sys.executable, '-c', CHANGE_RUN, str(path)]
    assert subprocess.run([*command, phase], timeout=15).returncode == 37
    result = json.loads(subprocess.check_output([*command, 'resume'], text=True, timeout=15))
    assert result == {'output': {'result': [8]}, 'nodes': ['grow', 'added']}
    assert sum(record['kind'] == 'topology' for record in SQLiteJournal(path).read()) == 1


def test_checkpointed_activation_resumes_with_the_same_round_and_explicit_node_progress(tmp_path):
    from interlacing import Node

    class Worker(Node):
        kind = 'test-worker'

        def __init__(self):
            super().__init__('worker', inputs=[Port('x')], outputs=[Port('y')])
            self.progress = 0

        def run(self, ctx, inputs):
            if ctx.resumed:
                assert ctx.round == 1 and inputs == {}
                return {'y': self.progress + 1}
            self.progress = inputs['x']
            ctx.emit('y', self.progress)
            assert ctx.commit()
            raise RuntimeError('crashed after progress save')

        def snapshot(self):
            return {'progress': self.progress}

        def restore(self, snapshot):
            self.progress = snapshot['progress']

    journal = SQLiteJournal(tmp_path / 'state.sqlite3', durable=True)
    with pytest.raises(RunError, match='crashed after progress save'):
        Engine(Chain(Worker()).build(), journal=journal).run({'in': 7})
    done = Engine(Chain(Worker()).build(), journal=journal).resume()
    assert done.outputs == {'out': [7, 8]} and done.rounds == {'worker': 1}


def test_artifact_deliveries_and_explicitly_unsupported_protocols(tmp_path):
    protocols = standard(DirectoryStore(tmp_path / 'artifacts'))
    source, receiver = function('source', lambda x: bytes([x])), function('receiver', lambda x: len(x))
    def build(protocol):
        return (GraphBuilder().add(source, receiver).input('in', 'source.x').connect('source.y', 'receiver.x', protocol)
                .output('out', 'receiver.y').compile(protocols))
    journal = SQLiteJournal(tmp_path / 'run.sqlite3', durable=True, checkpoint_every=2)
    with pytest.raises(GraphError, match='require value or artifact'):
        Engine(build('stream'), journal=journal)
    graph = build('artifact')
    assert Engine(graph, journal=journal).run({'in': 7}).outputs == {'out': [1]}
    assert Engine(graph, journal=journal).resume().outputs == {'out': [1]}
    view = explain_records({'nodes': [], 'edges': [], 'inputs': {}, 'outputs': {}},
                           journal.inspect(), 'done', [], timeline=True)
    assert view['provenance']['outputs']['out'][0][0]['message']['token'] == 'y#1'
