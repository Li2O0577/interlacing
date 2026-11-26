"""SQLite correction, targeted rework and interrupted rework recovery."""
import json
import os
import sqlite3
import subprocess
import sys
import threading

import pytest

from interlacing import Engine, FunctionNode, GraphBuilder, Port, SQLiteJournal, dump
from interlacing.engine.durable import DurableState
from interlacing.inspection import explain_records


def node(id, function, inputs=('x',), outputs=('y',), **options):
    return FunctionNode(id, function, inputs=[Port(p) if isinstance(p, str) else p for p in inputs],
                        outputs=[Port(p) for p in outputs], **options)


def quote_graph(seen):
    """Correct after the original calculation, with a one-delivery feedback edge."""
    def source(ctx, estimate=None, feedback=None):
        if estimate is not None:
            ctx.emit('quote', estimate)
        else:
            assert ctx.correct('quote#1', 7) == 'quote#1'
            assert ctx.correct('quote#1', 7) == 'quote#1'

    def total(numbers):
        seen.append(tuple(numbers))
        result = {'total': sum(numbers)}
        if numbers[0] == 7:
            result['detail'] = 'revised'
        return result

    producer = node('source', source, (Port('estimate', required=False), Port('feedback', required=False)),
                    ('quote',), context=True, firing='any')
    calc = node('calc', total, (Port('numbers', many=True),), ('total', 'detail'))
    return (GraphBuilder().add(producer, calc, node('fees', lambda x: x), node('unrelated', lambda x: x))
            .input('quote', 'source.estimate').input('fee', 'fees.x').input('other', 'unrelated.x')
            .connect('source.quote', 'calc.numbers').connect('fees.y', 'calc.numbers')
            .connect('calc.total', 'source.feedback', back=True, max_rounds=1)
            .output('quote', 'source.quote').output('total', 'calc.total').output('detail', 'calc.detail')
            .output('other', 'unrelated.y').compile())


INPUTS = {'quote': 2, 'fee': 3, 'other': 99}
REVISED = {'quote': [7], 'total': [10], 'detail': ['revised'], 'other': [99]}


def test_only_actual_readings_are_recomputed_and_their_slots_are_revised(tmp_path):
    journal = SQLiteJournal(tmp_path / 'run.sqlite3', durable=True, checkpoint_every=2)
    seen = []
    graph = quote_graph(seen)
    result = Engine(graph, journal=journal).run(INPUTS)
    assert result.outputs == REVISED
    assert result.rounds == {'source': 2, 'calc': 2, 'fees': 1, 'unrelated': 1}
    assert seen == [(2, 3), (7, 3)]
    records = journal.read()
    corrections = [r for r in records if r['kind'] == 'correct' and r['node'] == 'source']
    assert [(r['version'], r['noop']) for r in corrections] == [(2, False), (2, True)]
    taken = [r for r in records if r['kind'] == 'bind' and r['node'] == 'calc']
    assert taken[1]['redo'] == '1'
    assert taken[0]['took'][1] == taken[1]['took'][1]            # fee from the exact original delivery
    assert taken[0]['took'][0] != taken[1]['took'][0]

    at = records.index(corrections[0]) + 1
    assert DurableState.load(journal, at=at)[1].rework.jobs() == [
        {'node': 'calc', 'round': 1, 'took': taken[1]['took']}]
    assert DurableState.load(journal)[1].rework.jobs() == []
    view = explain_records(dump(graph), records, 'done', [], timeline=True)
    lineage = view['provenance']['outputs']['total'][0]
    assert lineage[1]['replaces_event_seq'] == lineage[0]['event_seq']
    assert [entry['message']['version'] for entry in lineage] == [1, 2]
    assert Engine(quote_graph([]), journal=journal).resume().outputs == REVISED


@pytest.mark.parametrize('rule', ['ignore', 'tell', 'rerun'])
def test_read_delivery_obeys_the_receivers_policy(tmp_path, rule):
    ran = []

    def producer(ctx, start=None, feedback=None):
        if start is not None:
            ctx.emit('y', start)
        else:
            ctx.correct('y#1', 8)

    def consumer(ctx, x):
        ran.append((x, ctx.replaces('x')))
        return x

    sender = node('sender', producer, (Port('start', required=False), Port('feedback', required=False)),
                  context=True, firing='any')
    receiver = node('receiver', consumer, context=True)
    receiver.on_correction = rule
    graph = (GraphBuilder().add(sender, receiver).input('in', 'sender.start')
             .connect('sender.y', 'receiver.x').connect('receiver.y', 'sender.feedback', back=True, max_rounds=1)
             .output('out', 'receiver.y').compile())
    journal = SQLiteJournal(tmp_path / 'run.sqlite3', durable=True)
    result = Engine(graph, journal=journal).run({'in': 1})
    assert result.outputs['out'] == {'ignore': [1], 'tell': [1, 8], 'rerun': [8]}[rule]
    assert [x for x, _ in ran] == ([1] if rule == 'ignore' else [1, 8])
    if rule == 'tell':
        assert ran[1][1] == 'sender.y>receiver.x#1'
    if rule == 'rerun':
        assert ran[1][1] is None


def test_unread_replacement_preserves_mailbox_order(tmp_path):
    def producer(ctx, x):
        first = ctx.emit('y', 1)
        ctx.emit('y', 2)
        ctx.correct(first, 9)

    graph = (GraphBuilder().add(node('sender', producer, context=True), node('receiver', lambda x: x))
             .input('in', 'sender.x').connect('sender.y', 'receiver.x').output('out', 'receiver.y').compile())
    journal = SQLiteJournal(tmp_path / 'run.sqlite3', durable=True)
    assert Engine(graph, journal=journal).run({'in': True}).outputs == {'out': [9, 2]}
    revision = next(r for r in journal.read() if r['kind'] == 'correct')
    assert revision['routes'] == ['unread']


def test_only_the_affected_round_and_its_downstream_readers_rework(tmp_path):
    def source(ctx, start=None, feedback=None):
        if start:
            ctx.emit('y', 1)
            ctx.emit('y', 2)
        else:
            ctx.correct('y#1', 9)

    def report(ctx, x):
        ctx.emit('y', x + 10)
        if x == 4:
            ctx.emit('feedback', True)

    producer = node('source', source, (Port('start', required=False), Port('feedback', required=False)),
                    context=True, firing='any')
    graph = (GraphBuilder().add(producer, node('calc', lambda x: x * 2),
                                node('report', report, outputs=('y', 'feedback'), context=True))
             .input('in', 'source.start').connect('source.y', 'calc.x').connect('calc.y', 'report.x')
             .connect('report.feedback', 'source.feedback', back=True, max_rounds=1)
             .output('result', 'report.y').compile())
    journal = SQLiteJournal(tmp_path / 'run.sqlite3', durable=True)
    result = Engine(graph, journal=journal).run({'in': True})
    assert result.outputs == {'result': [28, 14]}
    records = journal.read()
    assert [(r['node'], r['redo']) for r in records if r['kind'] == 'bind' and 'redo' in r] == [
        ('calc', '1'), ('report', '1')]


def test_superseded_original_and_rework_cannot_publish_late_results(tmp_path):
    started = {value: threading.Event() for value in (1, 2)}
    revised = {value: threading.Event() for value in (1, 2)}
    seen = []

    def sender(ctx, x):
        token = ctx.emit('y', 1)
        for value in (1, 2):
            assert started[value].wait(5)
            ctx.correct(token, value + 1)
            revised[value].set()

    def receiver(x):
        seen.append(x)
        if x in started:
            started[x].set()
            assert revised[x].wait(5)
        return x * 2

    graph = (GraphBuilder().add(node('sender', sender, context=True), node('receiver', receiver))
             .input('in', 'sender.x').connect('sender.y', 'receiver.x').output('out', 'receiver.y').compile())
    journal = SQLiteJournal(tmp_path / 'run.sqlite3', durable=True, checkpoint_every=2)
    result = Engine(graph, journal=journal, workers=2).run({'in': True})
    assert result.outputs == {'out': [6]} and seen == [1, 2, 3]
    records = journal.read()
    assert [r['round'] for r in records if r.get('stale')] == [1, 2]
    assert [r['round'] for r in records if r['kind'] == 'deliver' and r['node'] == 'receiver'] == [3]
    assert Engine(graph, journal=journal).resume().outputs == {'out': [6]}


def in_process(path, phase):
    journal = SQLiteJournal(path, durable=True, checkpoint_every=2)
    original = SQLiteJournal.write

    def cut(self, record):
        original(self, record)
        revision = record['kind'] == 'correct' and record.get('node') == 'source' and not record.get('noop')
        output = record['kind'] == 'deliver' and record.get('port') == 'detail'
        before = record['kind'] == 'bind' and record.get('node') == 'source' and record['round'] == 2
        if (phase, True) in [('revision', revision), ('output', output), ('old', before)]:
            os._exit(43)

    graph = quote_graph([])
    if phase == 'resume':
        result = Engine(graph, journal=journal).resume()
        print(json.dumps(result.outputs))
    else:
        SQLiteJournal.write = cut
        Engine(graph, journal=journal).run(INPUTS)


@pytest.mark.parametrize('phase', ['revision', 'output', 'old'])
def test_process_recovery_uses_revisions_and_resumes_rework_once(tmp_path, phase):
    path = tmp_path / 'run.sqlite3'
    command = [sys.executable, __file__, str(path)]
    assert subprocess.run([*command, phase], timeout=15).returncode == 43
    journal = SQLiteJournal(path, durable=True)
    if phase == 'old':
        # Simulate the previous release layout with its indexes absent from all checkpoints.
        with sqlite3.connect(path) as connection:
            rows = connection.execute('SELECT scope, seq, state FROM checkpoints').fetchall()
            for scope, seq, value in rows:
                state = json.loads(value)
                state.pop('rework')
                connection.execute('UPDATE checkpoints SET state = ? WHERE scope = ? AND seq = ?',
                                   (json.dumps(state), scope, seq))
            connection.execute('PRAGMA user_version = 3')
    else:
        assert DurableState.load(journal)[1].rework.jobs()[0]['node'] == 'calc'
    assert json.loads(subprocess.check_output([*command, 'resume'], text=True, timeout=15)) == REVISED
    records = journal.read()
    assert len([r for r in records if r['kind'] == 'bind' and r.get('redo')]) == 1
    assert len([r for r in records if r['kind'] == 'deliver' and r.get('port') == 'detail']) == 1
    assert DurableState.load(journal)[1].rework.jobs() == []
    with sqlite3.connect(path) as connection:
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        assert connection.execute('PRAGMA user_version').fetchone() == (4,)


if __name__ == '__main__':
    in_process(*sys.argv[1:])
