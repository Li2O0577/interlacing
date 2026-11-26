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


def wait_for_release(path):
    import time
    from pathlib import Path
    deadline = time.monotonic() + 10
    while not Path(path).is_file():
        if time.monotonic() >= deadline:
            raise TimeoutError("approval did not arrive during slow work")
        time.sleep(.02)
    return 17


def release_slow_after_approval(path):
    from pathlib import Path
    Path(path).touch()
    return "approved"


def test_review_can_be_submitted_before_parallel_sibling_finishes(service, tmp_path):
    import time
    db, lead, alice, _ = service
    graph = {'nodes': [{'id':'slow','kind':'function','function':__name__+'.wait_for_release'},
                       {'id':'fast','kind':'pass'}, {'id':'review','kind':'human'},
                       {'id':'publish','kind':'function','function':__name__+'.release_slow_after_approval'}],
             'inputs': {'path':'slow.path','quote':'fast.x'},
             'edges': [{'from':'fast.y','to':'review.request'}, {'from':'review.answer','to':'publish.path'}],
             'outputs': {'slow':'slow.out','approval':'publish.out'}}
    gate = tmp_path / 'release'
    run = lead('start', graph=graph, inputs={'path':str(gate),'quote':8}, assignments={'review':'Alice'})
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(work_one, db, 'first', allow=[__name__])
        try:
            deadline = time.monotonic() + 8
            while not (tasks := alice('tasks')):
                assert time.monotonic() < deadline
                time.sleep(.02)
            assert not future.done() and tasks[0]['request'] == 8
            alice('submit', task=tasks[0]['id'], version=1, answer=str(gate))
            assert future.result(timeout=6)['state'] == 'done'
        finally:
            gate.touch()
    assert work_one(db, 'next', allow=[__name__]) is None
    assert lead('status', **run)['result']['outputs'] == {'slow':[17], 'approval':['approved']}
    events = [v['record'] for v in lead('history', **run)]
    assert [v.get('node') for v in events if v['kind'] == 'act'].count('slow') == 1


def temporary_failure(marker):
    from pathlib import Path
    path = Path(marker)
    if not path.exists():
        path.touch()
        raise TimeoutError("busy")
    return 42


def test_client_preflight_and_retry_contract(service, tmp_path, monkeypatch):
    from interlacing.collab.client import Client, ServiceError
    db, _, _, _ = service
    monkeypatch.setenv('no_proxy', '127.0.0.1')
    token = Collaboration(db).bootstrap(str(uuid4()), 'operator')['token']
    with server(db, 0) as http:
        thread = threading.Thread(target=http.serve_forever)
        thread.start()
        try:
            client = Client('http://127.0.0.1:' + str(http.server_port), token)
            bad = {'method':'chain','nodes':[{'kind':'block','id':'inner','graph':{
                'edges':[{'from':'p.x','to':'q.y','protocol':'stream'}]}}]}
            for action in ('validate', 'start'):
                with pytest.raises(ServiceError, match=r'graph.nodes\[0\].graph.edges\[0\]') as error:
                    client.request(action, graph=bad)
                assert error.value.status == 400 and error.value.code == 'invalid_request'
            assert work_one(db, 'empty') is None
            graph = {'method':'chain','nodes':[{'kind':'function','id':'fetch',
                      'function':__name__+'.temporary_failure', 'retry_attempts':2}]}
            assert client.request('validate', graph=graph)['workers'] == 4
            run = client.request('start', graph=graph, inputs={'in':str(tmp_path/'once')}, workers=0)
            assert work_one(db, 'retry', allow=[__name__])['state'] == 'done'
            assert client.request('status', **run)['result']['outputs'] == {'out':[42]}
            history = client.request('history', **run)
            assert list(client.events(**run, follow=True, after=1)) == history[1:]
            attempts = [e['record']['value'] for e in history if e['record']['kind']=='once']
            assert len(attempts) == 2 and attempts[0]['timeout'] and attempts[1]['value'] == 42
        finally:
            http.shutdown()
            thread.join()


def test_two_forks_preserve_parent_answers_and_project_boundary(service):
    db, lead, alice, _ = service
    parent = lead('start', graph=REVIEW, inputs={'x':'original'}, assignments={'review':'Alice'})
    assert work_one(db,'first')['state'] == 'waiting'
    pos = lead('checkpoints', **parent)
    assert pos['checkpoints'] == [1, pos['last_event']]
    with pytest.raises(ValueError, match='unfinished'):
        lead('fork', **parent, at=pos['last_event'])
    task, = alice('tasks')
    alice('submit', task=task['id'], version=1, answer=10)
    assert work_one(db,'second')['state'] == 'done'
    before = lead('history', **parent)
    preview = lead('fork', **parent, at=1)
    assert lead('status', run=preview['run'])['state'] == 'preview'
    assert work_one(db,'idle') is None
    outside = Collaboration(db)
    stranger = outside.bootstrap(str(uuid4()),'lead')['token']
    with pytest.raises(Refused):
        outside.request(stranger, 'fork', {**parent, 'at':1})
    for answer in (20, 30):
        fork = lead('fork', **parent, at=1, inputs={'x':str(answer)}, execute=True)
        assert fork['origin']['run'] == parent['run']
        assert work_one(db,'branch')['state'] == 'waiting'
        pending, = [t for t in alice('tasks') if t['run'] == fork['run']]
        assert pending['request'] == str(answer) and pending['id'] != task['id']
        alice('submit', task=pending['id'], version=1, answer=answer)
        assert work_one(db,'finish')['state'] == 'done'
        assert lead('status', run=fork['run'])['result']['outputs']['answer'] == [answer]
    finished = lead('fork', **parent, at=before[-1]['seq'], execute=True)
    assert work_one(db,'settled')['state'] == 'done'
    assert lead('status', run=finished['run'])['result']['outputs']['answer'] == [10]
    assert not any(t['run'] == finished['run'] for t in alice('tasks'))
    assert lead('history', **parent) == before
    assert lead('status', **parent)['result']['outputs']['answer'] == [10]


def test_remote_scope_history_and_private_resource_search(service):
    db, lead, alice, bob = service
    for version in range(1, 4):
        assert alice('put', namespace='personal', name='notes', data={'body':'Tide'})['version'] == version
    alice('put', namespace='personal', name='other', data='wind')
    first = alice('resources', namespace='personal', query='tide', limit=1)
    assert first == {'items':[{'name':'notes','version':1,'kind':'json'}], 'next':['notes',1]}
    second = alice('resources', namespace='personal', query='tide', after=first['next'])
    assert [r['version'] for r in second['items']] == [2, 3] and second['next'] is None
    assert bob('resources', namespace='personal', query='tide')['items'] == []
    graph = {'method':'chain','nodes':[{'id':'outer','kind':'block','graph':{
        'method':'chain','nodes':[{'id':'inner','kind':'block','graph':REVIEW}]}}]}
    run = lead('start', graph=graph, inputs={'in':'approve'})
    assert work_one(db,'wait')['state'] == 'waiting'
    scope = [['outer',1],['inner',1]]
    assert lead('history', **run, scope=scope)[-1]['record']['kind'] == 'bind'
    assert 'outer' in lead('inspect', **run)['pending']
    task, = alice('tasks')
    alice('submit', **alice('claim', task=task['id'], version=1), answer=17)
    assert work_one(db,'finish')['state'] == 'done'
    archived = lead('history', **run, scope=scope)
    assert archived and all(item['archived'] for item in archived)
    assert any(item['record'].get('node') == 'review' for item in archived)
    assert lead('inspect', **run)['outputs'] == {'out':[17]}
    assert lead('inspect', **run, at=1)['outputs'] == {}
    with pytest.raises(Refused):
        bob('inspect', **run)


def test_interrupted_retry_respects_attempt_budget(service, tmp_path, monkeypatch):
    from pathlib import Path
    db, lead, _, _ = service
    module = tmp_path / 'business_service.py'
    module.write_text("from pathlib import Path\ndef unavailable(marker):\n"
                      "    with Path(marker).open('a') as f: f.write('attempt\\n')\n"
                      "    raise ConnectionError('offline')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    marker = tmp_path / 'attempts'
    graph = {'method':'chain','nodes':[{'id':'fetch','kind':'function','function':'business_service.unavailable',
                                        'retry_attempts':2}]}
    run = lead('start', graph=graph, inputs={'in':str(marker)})
    code = """
import os
from interlacing.collab.database import Database, PostgresJournal
from interlacing.collab.worker import work_one
write=PostgresJournal.write
def cut(self, record):
    write(self, record)
    if record['kind'] == 'once':
        os._exit(43)
PostgresJournal.write=cut
work_one(Database(os.environ['INTERLACING_TEST_POSTGRES']), 'first', allow=['business_service'])
"""
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), os.environ.get('PYTHONPATH','')]))
    assert subprocess.run([sys.executable,'-c',code], env=env, timeout=15).returncode == 43
    with db.connect() as conn:
        conn.execute("UPDATE il_runs SET expires=clock_timestamp() WHERE id=%s", (run['run'],))
    assert work_one(db,'next',allow=['business_service'])['state'] == 'failed'
    assert 'exhausted 2 attempts' in lead('status', **run)['result']['error']
    assert Path(marker).read_text().splitlines() == ['attempt', 'attempt']


def test_schema_upgrade_preserves_previous_run_configuration(service):
    db, *_ = service
    schema = 'legacy_' + uuid4().hex
    with db.connect() as conn:
        conn.execute(f'CREATE SCHEMA {schema}')
    legacy = Database(db.driver.conninfo.make_conninfo(db.dsn, options=f'-c search_path={schema}'))
    try:
        legacy.install()
        with legacy.connect() as conn:
            conn.execute('ALTER TABLE il_runs DROP COLUMN workers, DROP COLUMN description, DROP COLUMN origin')
            conn.execute('UPDATE il_schema SET version=1')
            conn.execute("INSERT INTO il_projects VALUES ('team')")
            conn.execute("INSERT INTO il_runs(id,project,graph,inputs,resources,assignments) "
                         "VALUES (%s,'team','{}','{}','{}','{}')", (str(uuid4()),))
        legacy.install()
        legacy.install()
        with legacy.connect() as conn:
            assert conn.execute('SELECT workers,origin FROM il_runs').fetchone() == {'workers':0,'origin':None}
            assert conn.execute('SELECT version FROM il_schema').fetchone()['version'] == 2
    finally:
        with db.connect() as conn:
            conn.execute(f'DROP SCHEMA {schema} CASCADE')
