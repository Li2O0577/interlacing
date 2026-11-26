"""Replay the same value/asset challenge graph across real process exits."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from interlacing import Engine, FunctionNode, GraphBuilder, SQLiteJournal, standard
from interlacing.protocols import DirectoryStore


def quote(x):
    return x


def verify(ctx, v):
    if ctx.challengeable('v'):
        reply = ctx.reject('v', 'tax?')
        if reply['answer'] == 'corrected':
            return reply['value']
    return v


def build(protocol, protocols, explain):
    sender = FunctionNode.of(quote, 's')
    def challenged(ctx, request):
        assert request['content'] == 100 and request['message'] == 'out#1'
        if explain:
            return 'tax included'
        ctx.correct(request['message'], 113)
    sender.challenged = challenged
    g = GraphBuilder().add(sender, FunctionNode.of(verify, 'd'))
    return g.input('in', 's.x').connect('s.out', 'd.v', protocol, challenges=1).output('out', 'd.out').compile(protocols)


def drive(directory, protocol, phase, explain):
    protocols = standard(DirectoryStore(directory / 'assets'))
    graph = build(protocol, protocols, explain)
    journal = SQLiteJournal(directory / 'history', durable=True, checkpoint_every=1)
    if phase == 'standard':
        return Engine(graph, protocols).run({'in': 100}).outputs
    write = journal.write
    def cut(event):
        write(event)
        if (event['kind'] == phase or phase == 'answer_bind' and 'answering' in event):
            os._exit(40)
    journal.write = cut
    return Engine(graph, protocols, workers=2, journal=journal).go({'in': 100}).outputs


@pytest.mark.parametrize('protocol', ['value', 'artifact'])
@pytest.mark.parametrize('phase,explain', [('challenge', False), ('answer_bind', False),
                                           ('correct', False), ('review_reply', True)])
def test_recover_exact_delivery_and_reply(tmp_path, protocol, phase, explain):
    expected = drive(tmp_path, protocol, 'standard', explain)
    cmd = [sys.executable, __file__, str(tmp_path), protocol, phase, str(int(explain))]
    assert subprocess.run(cmd, timeout=15).returncode == 40
    cmd[-2] = 'resume'
    assert json.loads(subprocess.check_output(cmd, timeout=15)) == expected
    records = SQLiteJournal(tmp_path / 'history').read()
    assert [r['number'] for r in records if r['kind'] == 'challenge'] == [1]
    assert len([r for r in records if r['kind'] == ('review_reply' if explain else 'correct')]) == 1
    assert len([r for r in records if r['kind'] == 'bind' and r['node'] == 'd']) == 1


def test_durable_challenge_budget(tmp_path):
    def ask(ctx, v):
        return [ctx.reject('v', 'please explain')['answer'] for _ in range(3)]
    sender = FunctionNode.of(quote, 's')
    g = (GraphBuilder().add(sender, FunctionNode.of(ask, 'd')).input('in', 's.x')
         .connect('s.out', 'd.v', challenges=2).output('out', 'd.out').compile())
    result = Engine(g, journal=SQLiteJournal(tmp_path / 'j', durable=True)).run({'in': 1})
    assert result.last('out') == ['explained', 'explained', 'refused']


@pytest.mark.parametrize('same', [False, True])
def test_artifact_binary_reply_and_same_value(tmp_path, same):
    def send(x):
        return b'old bytes'

    def ask(ctx, x):
        result = ctx.reject('x', 'please check this asset')
        return [result['answer'], result['value'].decode()]

    sender = FunctionNode.of(send, 'send')
    def revise(ctx, request):
        assert request['content'] == b'old bytes'
        ctx.correct(request['message'], b'old bytes' if same else b'new bytes')
    sender.challenged = revise
    protocols = standard(DirectoryStore(tmp_path / 'assets'))
    g = (GraphBuilder().add(sender, FunctionNode.of(ask, 'ask')).input('seed', 'send.x')
         .connect('send.out', 'ask.x', 'artifact', challenges=1).output('result', 'ask.out').compile(protocols))
    expected = Engine(g, protocols).run({'seed': 0}).outputs
    journal = SQLiteJournal(tmp_path / 'run', durable=True, checkpoint_every=1)
    assert Engine(g, protocols, journal=journal).run({'seed': 0}).outputs == expected
    records = journal.read()
    assert any(r.get('answers') for r in records)
    assert 'old bytes' not in json.dumps([r for r in records if r['kind'] == 'challenge'])
    assert not journal.recovery()[0]['reviews']


def test_correction_wakes_questioner_and_recomputes_only_other_actual_reader(tmp_path):
    import threading
    consumed = threading.Event()
    counts = []
    def report(x):
        counts.append(x)
        consumed.set()
        return 2*x
    def question(ctx, x):
        assert consumed.wait(5)
        return ctx.reject('x', 'tax?')['value']
    sender = FunctionNode.of(quote, 'send')
    sender.challenged = lambda ctx, r: ctx.correct(r['message'], 113)
    graph = (GraphBuilder().add(sender, FunctionNode.of(report, 'report'), FunctionNode.of(question, 'ask'))
             .input('seed', 'send.x').connect('send.out', 'report.x')
             .connect('send.out', 'ask.x', challenges=1).output('total', 'report.out')
             .output('price', 'ask.out').compile())
    journal = SQLiteJournal(tmp_path / 'j', durable=True)
    result = Engine(graph, workers=4, journal=journal).run({'seed': 100})
    assert result.outputs == {'price': [113], 'total': [226]}
    assert counts == [100, 113] and result.rounds['ask'] == 1


def test_async_reject_and_explain(tmp_path):
    import asyncio
    async def ask(ctx, x):
        return await ctx.areject('x', 'units?')
    sender = FunctionNode.of(quote, 'send')
    async def explain(ctx, request):
        await asyncio.sleep(0)
        ctx.explain(request['id'], 'metres')
    sender.challenged = explain
    graph = (GraphBuilder().add(sender, FunctionNode.of(ask, 'ask')).input('seed', 'send.x')
             .connect('send.out', 'ask.x', challenges=1).output('out', 'ask.out').compile())
    result = asyncio.run(Engine(graph, journal=SQLiteJournal(tmp_path / 'j', durable=True)).arun({'seed': 1}))
    assert result.last('out') == {'answer': 'explained', 'text': 'metres'}


if __name__ == '__main__':
    print(json.dumps(drive(Path(sys.argv[1]), sys.argv[2], sys.argv[3], bool(int(sys.argv[4])))))
