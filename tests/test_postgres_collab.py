"""Integration contracts for the service. Set INTERLACING_TEST_POSTGRES to a disposable PostgreSQL database."""
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from uuid import uuid4

import pytest

from interlacing.collab.cli import server
from interlacing.collab.database import Database, LostLease, PostgresJournal, Refused
from interlacing.collab.service import Collaboration
from interlacing.collab.worker import execute, work_one

DSN = os.environ.get('INTERLACING_TEST_POSTGRES')
pytestmark = pytest.mark.skipif(not DSN, reason='set INTERLACING_TEST_POSTGRES to run the PostgreSQL integration tests')

REVIEW = {'nodes': [{'id': 'review', 'kind': 'human', 'outputs': [{'name': 'answer', 'schema': 'int'}]}],
          'inputs': {'x': 'review.request'}, 'outputs': {'answer': 'review.answer'}}


@pytest.fixture
def service(tmp_path, monkeypatch):
    monkeypatch.setenv("INTERLACING_CONTENT_DIR", str(tmp_path / "content"))
    database = Database(DSN)
    database.install()
    collab = Collaboration(database)
    name = str(uuid4())
    manager = collab.bootstrap(name, 'lead')['token']

    def client(token):
        def request(action, **args):
            return collab.request(token, action, args)
        return request

    lead = client(manager)
    a = client(lead('member', name='Alice')['token'])
    b = client(lead('member', name='Bob')['token'])
    return database, lead, a, b


def invoke_worker():
    return json.loads(subprocess.check_output(
        [sys.executable, '-m', 'interlacing.cli', 'collab', 'worker', '--once'],
        env={**os.environ, 'INTERLACING_POSTGRES_DSN': DSN}, text=True, timeout=25))


def race(*ops):
    gate = threading.Barrier(len(ops))

    def attempt(operation):
        gate.wait(5)
        try:
            return operation()
        except Refused:
            return None
    with ThreadPoolExecutor(len(ops)) as pool:
        return list(pool.map(attempt, ops))


def test_concurrent_claim_submit_and_resume_across_processes(service):
    db, lead, a, b = service
    run = lead('start', graph=REVIEW, inputs={'x': 'quote'})
    assert invoke_worker()['state'] == 'waiting'
    task = a('tasks')[0]
    claims = race(*(lambda client=c: client('claim', task=task['id'], version=1) for c in (a, b)))
    assert sum(x is not None for x in claims) == 1
    winner, loser = (a, b) if claims[0] else (b, a)
    receipt = next(x for x in claims if x)
    with pytest.raises(Refused):
        loser('submit', **receipt, answer=3)
    with pytest.raises(ValueError):
        winner('submit', **receipt, answer='wrong type')
    replies = race(*(lambda: winner('submit', **receipt, answer=7) for _ in range(2)))
    assert replies[0] == replies[1] == {'task': task['id'], 'version': 3}
    with pytest.raises(Refused):
        winner('submit', **receipt, answer=8)
    assert invoke_worker()['state'] == 'done'
    assert invoke_worker() is None
    assert lead('status', **run)['result']['outputs'] == {'answer': [7]}
    assert [e['action'] for e in lead('audit')].count('task.submit') == 1
    assert [e['record']['kind'] for e in lead('history', **run)].count('deliver') == 1
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM il_commands WHERE run=%s AND done",
                            (run['run'],)).fetchone()['n'] == 2


def test_parallel_human_nodes_are_visible_together_and_answers_are_private(service):
    db, lead, a, b = service
    graph = {'nodes': [{'id': n, 'kind': 'human'} for n in ('review1','review2')],
             'inputs': {'first': 'review1.request', 'second': 'review2.request'},
             'outputs': {'first': 'review1.answer', 'second': 'review2.answer'}}
    run = lead('start', graph=graph, inputs={'first': 'Alice only', 'second': 'Bob only'},
               assignments={'review1': 'Alice', 'review2': 'Bob'})
    assert work_one(db, 'one')['state'] == 'waiting'
    assert len(lead('tasks')) == 2
    first, = a('tasks')
    second, = b('tasks')
    assert first['request'] == 'Alice only' and second['request'] == 'Bob only'
    moved = lead('assign', task=first['id'], version=first['version'], member='Bob')
    with pytest.raises(Refused):
        a('submit', task=first['id'], version=1, answer=10)
    b('submit', **moved, answer=10)
    assert work_one(db, 'two')['state'] == 'waiting'
    b('submit', task=second['id'], version=second['version'], answer=20)
    assert work_one(db, 'three')['state'] == 'done'
    assert lead('status', **run)['result']['outputs'] == {'first': [10], 'second': [20]}


def test_old_generation_cannot_write_after_a_process_dies(service):
    db, lead, _, _ = service
    run = lead('start', graph=REVIEW, inputs={'x': 'crash'})
    code = """
import os, sys
from interlacing.collab.database import Database, PostgresJournal
from interlacing.collab.worker import execute
db=Database(os.environ['INTERLACING_TEST_POSTGRES'])
lease=db.claim('crashed', 30)
assert lease.run == sys.argv[1]
write=PostgresJournal.write
def cut(self, record):
    write(self,record)
    if record['kind']=='bind':
        os._exit(43)
PostgresJournal.write=cut
execute(db,lease)
"""
    assert subprocess.run([sys.executable, '-c', code, run['run']], timeout=15).returncode == 43
    with db.connect() as conn:
        conn.execute("UPDATE il_runs SET expires=clock_timestamp()-interval '1 second' WHERE id=%s", (run['run'],))
        command = conn.execute('SELECT id FROM il_commands WHERE run=%s', (run['run'],)).fetchone()['id']
    from interlacing.collab.database import Lease
    old = Lease(run['run'], command, 1, 'crashed')
    lease = db.claim('replacement', 30)
    assert lease.generation == 2 and lease.command == old.command
    journal = PostgresJournal(db, old)
    for op in (lambda: db.renew(old, 10), lambda: db.finish(old, 'done', {}), lambda: journal.write({}),
               lambda: journal.checkpoint({'seq': 2}), journal.reset, journal.inner('x', 1).discard):
        with pytest.raises(LostLease):
            op()
    assert execute(db, lease)['state'] == 'waiting'
    assert [e['record']['kind'] for e in lead('history', **run)] == ['run','bind']
    task = lead('tasks')[0]
    owned = lead('claim', task=task['id'], version=1)
    lead('submit', **owned, answer=5)
    assert work_one(db, 'finisher')['state'] == 'done'


def test_resources_project_access_and_independent_workers(service):
    db, lead, a, b = service
    other = Collaboration(db)
    outside = other.bootstrap(str(uuid4()), 'outsider')['token']
    ref = lead('put', namespace='shared', name='price', data=5)
    lead('put', namespace='shared', name='price', data=17)
    private = a('put', namespace='personal', name='note', file=base64.b64encode(b'private').decode())
    assert base64.b64decode(a('get', **private)['file']) == b'private'
    with pytest.raises(Refused):
        b('get', **private)
    with pytest.raises(Refused):
        a('put', namespace='shared', name='price', data=9)
    with pytest.raises(Refused):
        other.request(outside, 'get', ref)
    graph = {'method': 'chain', 'nodes': [{'id': 'passage','kind': 'pass'}]}
    runs = [lead('start', graph=graph, resources={'in': ref}) for _ in range(2)]
    with ThreadPoolExecutor(2) as pool:
        outcomes = list(pool.map(lambda _: invoke_worker(), range(2)))
    assert {r['run'] for r in outcomes} == {r['run'] for r in runs}
    for run in runs:
        assert lead('status', **run)['result']['outputs'] == {'out':[5]}
        with pytest.raises(Refused):
            other.request(outside, 'history', run)


def test_http_endpoint_and_bad_credentials(service):
    db, _, _, _ = service
    token = Collaboration(db).bootstrap(str(uuid4()), 'lead')['token']
    with server(db, 0) as http:
        thread = threading.Thread(target=http.serve_forever)
        thread.start()
        try:
            url = f'http://127.0.0.1:{http.server_port}/v1/request'
            def request(token):
                return urllib.request.Request(url, data=b'{"action":"tasks"}', headers={'Authorization':'Bearer '+token})
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            assert json.load(opener.open(request(token), timeout=5)) == []
            with pytest.raises(urllib.error.HTTPError) as error:
                opener.open(request('invalid'), timeout=5)
            assert error.value.code == 403
        finally:
            http.shutdown()
            thread.join()


def test_nested_wait_keeps_child_history_and_artifact_content_across_workers(service):
    db, lead, a, _ = service
    inner = {'nodes': [{'id': 'send', 'kind': 'pass'}, {'id': 'person', 'kind': 'human'}],
             'edges': [{'from': 'send.y', 'to': 'person.request', 'protocol': 'artifact'}],
             'inputs': {'in': 'send.x'}, 'outputs': {'out': 'person.answer'}}
    graph = {'method': 'chain', 'nodes': [{'kind': 'block', 'id': 'block', 'graph': inner}]}
    run = lead('start', graph=graph, inputs={'in': 'input file contents'}, assignments={'block.1/person':'Alice'})
    assert invoke_worker()['state'] == 'waiting'
    task, = a('tasks')
    assert task['node'] == 'block.1/person' and task['request'] == 'input file contents'
    a('submit', task=task['id'], version=1, answer='accepted')
    assert invoke_worker()['state'] == 'done'
    assert lead('status', **run)['result']['outputs'] == {'out': ['accepted']}
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM il_events WHERE run=%s AND scope<>'[]'::jsonb",
                            (run['run'],)).fetchone()['n'] == 0
    records = [event['record'] for event in lead('history', **run)]
    assert any(r.get('child_history') for r in records)


def test_task_answer_rolls_back_if_resume_command_cannot_be_inserted(service):
    db, lead, _, _ = service
    run = lead('start', graph=REVIEW, inputs={'x': 'rollback'})
    work_one(db, 'first')
    task, = lead('tasks')
    owned = lead('claim', task=task['id'], version=1)
    # Force the command insert to fail after the answer update, within the same transaction.
    key = 'answer/' + owned['task']
    with db.connect() as conn:
        conn.execute("INSERT INTO il_commands(run,key,done) VALUES (%s,%s,true)", (run['run'], key))
    with pytest.raises(db.driver.IntegrityError):
        lead('submit', **owned, answer=4)
    assert lead('tasks')[0]['state'] == 'claimed'
    assert not any(e['action'] == 'task.submit' for e in lead('audit'))
    with db.connect() as conn:
        conn.execute("DELETE FROM il_commands WHERE run=%s AND key=%s", (run['run'], key))
    lead('submit', **owned, answer=4)
    assert work_one(db, 'second')['state'] == 'done'


def test_postgres_checkpoint_replays_ordered_tail_and_scope_cleanup(service):
    db, lead, _, _ = service
    lead('start', graph=REVIEW)
    lease = db.claim('history', 30)
    root = PostgresJournal(db, lease)
    left, right = root.inner('branch', 1), root.inner('branch', 2)
    left.write({'kind': 'a'})
    left.checkpoint({'seq': 1, 'mail': {'z': 1, 'a': 2}})
    left.write({'kind': 'b'})
    left.inner('branch', 1).write({'nested': True})
    right.write({'sibling': True})
    snapshot, tail = left.recovery()
    assert list(snapshot['mail']) == ['z', 'a'] and tail == [{'kind':'b'}]
    assert left.recovery(at=1)[1] == []
    left.discard()
    assert left.read() == left.inner('branch', 1).read() == []
    assert right.read() == [{'sibling': True}]
    db.finish(lease, 'done', {})


def publish_quote(ctx, initial=None, edit=None):
    if initial is not None:
        ctx.emit('quote', initial)
    else:
        ctx.correct('quote#1', edit)


def test_input_revision_supersedes_pending_human_task_and_reissues_it(service):
    db, lead, a, _ = service
    graph = {'nodes': [{'id': 'source', 'kind': 'function', 'function': __name__ + '.publish_quote',
                        'context': True, 'firing': 'any', 'inputs': [{'name':'initial','required':False},
                        {'name':'edit','required':False}], 'outputs':['quote']},
                       {'id':'review', 'kind':'human'}, {'id':'editor','kind':'human'}],
             'inputs': {'initial':'source.initial','edit':'editor.request'},
             'edges': [{'from':'source.quote','to':'review.request'}, {'from':'editor.answer','to':'source.edit'}],
             'outputs': {'accepted':'review.answer'}}
    run = lead('start', graph=graph, inputs={'initial':2,'edit':'correct quote'},
               assignments={'editor':'lead','review':'Alice'})
    def drive():
        return work_one(db, 'worker', allow=[__name__])
    assert drive()['state'] == 'waiting'
    old, = a('tasks')
    editor = next(t for t in lead('tasks') if t['node'] == 'editor')
    lead('submit', task=editor['id'], version=1, answer=7)
    drive()
    while drive() is not None:
        pass
    with pytest.raises(Refused):
        a('submit', task=old['id'], version=1, answer='out of date')
    current, = [t for t in a('tasks') if t['state'] == 'claimed']
    assert current['request'] == 7 and current['id'] != old['id']
    a('submit', task=current['id'], version=current['version'], answer='accepted 7')
    assert drive()['state'] == 'done'
    assert lead('status', **run)['result']['outputs'] == {'accepted':['accepted 7']}
