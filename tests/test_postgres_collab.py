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


def test_invalid_assignment_never_opens_a_task(service):
    db, lead, alice, bob = service
    inner = {'nodes': [*REVIEW['nodes'], {'id': 'ordinary', 'kind': 'pass'}],
             'edges': [{'from': 'ordinary.y', 'to': 'review.request'}],
             'inputs': {'x': 'ordinary.x'}, 'outputs': REVIEW['outputs']}
    graph = {'method': 'chain', 'nodes': [{'kind': 'graph', 'id': 'group', 'graph': 'inner'}],
             'graphs': {'inner': inner}}
    for key in ('typo', 'group.0/review', 'group.01/review', 'group/review', 'group.1/reveiw',
                'group.1/ordinary', 'group.1/review.1/extra', 'group'):
        for action in ('start', 'validate'):
            with pytest.raises(ValueError) as error:
                lead(action, graph=graph, assignments={key: 'Alice'})
            assert repr(key) in str(error.value)
    with pytest.raises(Refused, match='unknown task assignee'):
        lead('validate', graph=graph, assignments={'group.1/review': 'unknown'})
    assert work_one(db, 'idle') is None
    assert not lead('tasks') and 'run.start' not in {item['action'] for item in lead('audit')}
    assignments = {'group.1/review': 'Alice'}
    assert 'assignment_paths' in lead('validate', graph=graph, assignments=assignments)['checked']
    run = lead('start', graph=graph, inputs={'in': 'private'}, assignments=assignments)
    assert work_one(db, 'open')['state'] == 'waiting', lead('status', **run)
    task, = alice('tasks')
    assert lead('tasks') == [task] and not bob('tasks')
    with pytest.raises(Refused):
        bob('claim', task=task['id'], version=task['version'])


def test_generated_assignments_checked_before_any_node_runs(service):
    db, lead, alice, bob = service
    graph = {'method': 'fan', 'nodes': [{'kind': 'human', 'id': 'review', 'times': 2}]}
    def start(key):
        assignment = {key: 'Alice'}
        checks = lead('validate', graph=graph, assignments=assignment)
        assert 'assignment_paths' in checks['worker_checks']
        return lead('start', graph=graph, assignments=assignment, inputs={'in': 'review this'})

    invalid = start('review-3')
    assert work_one(db, 'check')['state'] == 'failed'
    assert 'review-3' in lead('status', **invalid)['result']['error']
    assert lead('history', **invalid) == [] and not lead('tasks')
    start('review-1')
    assert work_one(db, 'dispatch')['state'] == 'waiting'
    tasks = {task['node']: task for task in alice('tasks')}
    assert tasks['review-1']['assignee'] == 'Alice'
    assert bob('tasks') == [tasks['review-2']]
    bob('claim', task=tasks['review-2']['id'], version=tasks['review-2']['version'])


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
            with opener.open(url.rsplit('/', 2)[0] + '/review') as page:
                assert page.headers.get_content_type() == 'text/html'
                assert '任务审核' in page.read().decode()
            from interlacing.collab.client import Client, ServiceError
            client = Client(url.rsplit('/', 2)[0], token)
            args = dict(graph=REVIEW, inputs={'x': 'quote'}, request_key='http-test')
            run = client.request('start', **args)
            with pytest.raises(ServiceError) as conflict:
                client.request('start', **{**args, 'workers': 0})
            assert (conflict.value.status, conflict.value.code) == (409, 'request_key_conflict')
            assert work_one(db, 'http-worker')['run'] == run['run']
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


@pytest.mark.parametrize("nested", [False, True])
def test_review_can_be_submitted_before_parallel_sibling_finishes(service, tmp_path, nested):
    import time
    db, lead, alice, _ = service
    graph = {'nodes': [{'id':'slow','kind':'function','function':__name__+'.wait_for_release'},
                       {'id':'fast','kind':'pass'}, {'id':'review','kind':'human'},
                       {'id':'publish','kind':'function','function':__name__+'.release_slow_after_approval'}],
             'inputs': {'path':'slow.path','quote':'fast.x'},
             'edges': [{'from':'fast.y','to':'review.request'}, {'from':'review.answer','to':'publish.path'}],
             'outputs': {'slow':'slow.out','approval':'publish.out'}}
    assignment = 'review'
    if nested:
        graph = {'nodes': [{'id': 'group', 'kind': 'block', 'graph': graph}],
                 'inputs': {'path': 'group.path', 'quote': 'group.quote'},
                 'outputs': {'slow': 'group.slow', 'approval': 'group.approval'}}
        assignment = 'group.1/review'
    gate = tmp_path / 'release'
    run = lead('start', graph=graph, inputs={'path':str(gate),'quote':8}, assignments={assignment:'Alice'})
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
    history = lead('history', **run, scope=[['group', 1]] if nested else [])
    events = [v.get('record', v) for v in history]
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
                      "    if Path(marker + '.ready').exists(): return 42\n"
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
    lead('retry', **run)
    cut_retry = code.replace("record['kind'] == 'once'", "record.get('name', '').endswith('-attempt-1')")
    assert subprocess.run([sys.executable, '-c', cut_retry], env=env, timeout=15).returncode == 43
    with db.connect() as conn:
        conn.execute("UPDATE il_runs SET expires=clock_timestamp() WHERE id=%s", (run['run'],))
    assert work_one(db, 'still-offline', allow=['business_service'])['state'] == 'failed'
    assert len(marker.read_text().splitlines()) == 4
    Path(str(marker) + '.ready').touch()
    lead('retry', **run)
    with pytest.raises(Refused):
        lead('retry', **run)
    assert work_one(db, 'recovered', allow=['business_service'])['state'] == 'done'
    assert lead('status', **run)['result']['outputs'] == {'out': [42]}
    assert len(marker.read_text().splitlines()) == 5


def test_schema_upgrade_preserves_previous_run_configuration(service):
    db, *_ = service
    schema = 'legacy_' + uuid4().hex
    with db.connect() as conn:
        conn.execute(f'CREATE SCHEMA {schema}')
    legacy = Database(db.driver.conninfo.make_conninfo(db.dsn, options=f'-c search_path={schema}'))
    try:
        legacy.install()
        with legacy.connect() as conn:
            conn.execute('ALTER TABLE il_runs DROP COLUMN shared_workers, DROP COLUMN workers, DROP COLUMN description, DROP COLUMN origin')
            conn.execute('UPDATE il_schema SET version=1')
            conn.execute("INSERT INTO il_projects VALUES ('team')")
            conn.execute("INSERT INTO il_runs(id,project,graph,inputs,resources,assignments) "
                         "VALUES (%s,'team','{}','{}','{}','{}')", (str(uuid4()),))
        legacy.install()
        legacy.install()
        with legacy.connect() as conn:
            assert conn.execute('SELECT workers,origin,shared_workers FROM il_runs').fetchone() == {
                'workers': 0, 'origin': None, 'shared_workers': False}
            assert conn.execute('SELECT version FROM il_schema').fetchone()['version'] == 3
    finally:
        with db.connect() as conn:
            conn.execute(f'DROP SCHEMA {schema} CASCADE')


def disputed_price(ctx, price):
    """Keep the sender active until both named readers have queried this exact output."""
    import time
    token = ctx.emit('out', price)
    received = {}
    deadline = time.monotonic() + 8
    while len(received) < 2:
        for question in ctx.rejections():
            assert question['message'] == token
            received[question['by']] = question
        if len(received) < 2:
            assert time.monotonic() < deadline
            time.sleep(.03)
    ctx.explain(received['terms']['id'], 'tax included')
    ctx.correct(token, price + 13)


def ask_for_price(ctx, x):
    reply = ctx.reject('x', 'confirm the price')
    return reply


def subtotal(x):
    return 2*x


def verify_terms(ctx, x, subtotal):
    assert subtotal == 2*x
    return ctx.reject('x', 'confirm the price')


@pytest.mark.parametrize('cut', ['challenge', 'correct'])
def test_challenge_worker_takeover_and_private_review(service, cut):
    db, lead, alice, bob = service
    def fn(id, name, **options):
        return {'kind': 'function', 'id': id, 'function': __name__ + '.' + name, **options}
    graph = {'nodes': [fn('quote', 'disputed_price'), fn('terms', 'verify_terms', on_correction='ignore'),
                       fn('invoice', 'subtotal'),
                       fn('total', 'ask_for_price'), {'id': 'audit', 'kind': 'human'},
                       {'id': 'independent', 'kind': 'human'}],
             'edges': [{'from': 'quote.out', 'to': n + '.x', 'challenges': 1} for n in ('terms', 'total')] +
                      [{'from': 'quote.out', 'to': 'invoice.x'}, {'from': 'invoice.out', 'to': 'terms.subtotal'},
                       {'from': 'total.out', 'to': 'audit.request'}],
             'inputs': {'seed': 'quote.price', 'separate': 'independent.request'},
             'outputs': {'terms': 'terms.out', 'total': 'total.out', 'invoice': 'invoice.out',
                         'approved': 'audit.answer', 'independent': 'independent.answer'}}
    from interlacing import Catalog, Engine, load
    inputs = {'seed': 100, 'separate': 'separate review'}
    catalog = Catalog(allow=[__name__], ask=lambda node, turn, request: {'audit': 'approved', 'independent': 'filed'}[node])
    standard = Engine(load(graph, catalog), workers=5).run(inputs)
    run = lead('start', graph=graph, inputs=inputs,
               assignments={'audit': 'Alice', 'independent': 'Bob'}, workers=5)
    code = """
import os,sys
from interlacing.collab.database import Database, PostgresJournal
from interlacing.collab.worker import work_one
db = Database(os.environ['INTERLACING_TEST_POSTGRES'])
write = PostgresJournal.write
def die(self, event):
    write(self, event)
    if event['kind'] == sys.argv[1]:
        os._exit(48)
PostgresJournal.write = die
work_one(db, 'interrupted', allow=[sys.argv[2]])
"""
    env = {**os.environ, 'PYTHONPATH': os.path.dirname(__file__)}
    assert subprocess.run([sys.executable, '-c', code, cut, __name__], env=env, timeout=20).returncode == 48
    from interlacing.collab.database import Lease
    with db.connect() as conn:
        old = conn.execute("SELECT c.id,r.generation FROM il_commands c JOIN il_runs r ON r.id=c.run "
                           "WHERE c.run=%s AND NOT c.done ORDER BY c.id LIMIT 1", (run['run'],)).fetchone()
        conn.execute("UPDATE il_runs SET expires=clock_timestamp()-interval '1 second' WHERE id=%s", (run['run'],))
    with pytest.raises(LostLease):
        PostgresJournal(db, Lease(run['run'], old['id'], old['generation'], 'interrupted')).write(
            {'kind': 'review_reply', 'id': 'late reply from expired worker'})
    assert work_one(db, 'replacement', allow=[__name__])['state'] == 'waiting', lead('status', **run)
    task, = alice('tasks')
    assert task['request'] == {'answer': 'corrected', 'value': 113}
    separate, = bob('tasks')
    assert separate['request'] == 'separate review'
    alice('submit', task=task['id'], version=task['version'], answer='approved')
    bob('submit', task=separate['id'], version=separate['version'], answer='filed')
    assert work_one(db, 'finish', allow=[__name__])['state'] == 'done'
    outputs = lead('status', **run)['result']['outputs']
    assert outputs == standard.outputs
    assert outputs == {'terms': [{'answer': 'explained', 'text': 'tax included'}], 'invoice': [226],
                       'total': [{'answer': 'corrected', 'value': 113}],
                       'approved': ['approved'], 'independent': ['filed']}
    records = [row['record'] for row in lead('history', **run, limit=500)]
    questions = [e for e in records if e['kind'] == 'challenge']
    assert len(questions) == 2 and {e['node'] for e in questions} == {'terms', 'total'}
    assert all(e['number'] == 1 and e['message'] == 'out#1' for e in questions)
    assert sum(e['kind'] == 'correct' and e['node'] == 'quote' for e in records) == 1
    rounds = lead('status', **run)['result']['rounds']
    assert rounds == standard.rounds
    assert rounds['invoice'] == 2 and rounds['total'] == rounds['independent'] == 1


def release_after_independent_review(path):
    from pathlib import Path
    Path(path).touch()
    return 'released'


def quote_until_release(ctx, path):
    from pathlib import Path
    import time
    ctx.emit('out', 100)
    deadline = time.monotonic() + 15
    while not Path(path).exists():
        assert time.monotonic() < deadline
        time.sleep(.025)


def test_independent_person_can_finish_while_price_is_disputed(service, tmp_path):
    import time
    db, lead, _, bob = service
    gate = tmp_path / 'reviewed'
    nodes = [{'kind': 'function', 'id': name, 'function': f'{__name__}.{fn}'} for name, fn in (
        ('quote', 'quote_until_release'), ('verify', 'ask_for_price'), ('release', 'release_after_independent_review'))]
    graph = {'nodes': [*nodes, {'kind': 'human', 'id': 'independent'}],
             'inputs': {'gate': 'quote.path', 'separate': 'independent.request'},
             'outputs': {'decision': 'verify.out', 'other': 'release.out'},
             'edges': [{'from': 'quote.out', 'to': 'verify.x', 'challenges': 1},
                       {'from': 'independent.answer', 'to': 'release.path'}]}
    run = lead('start', graph=graph, inputs={'gate': str(gate), 'separate': 'check the file'},
               assignments={'independent': 'Bob'})
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(work_one, db, 'both-branches', allow=[__name__])
        try:
            deadline = time.monotonic() + 10
            while True:
                history = lead('history', **run)
                tasks = bob('tasks')
                if any(row['record']['kind'] == 'challenge' for row in history) and tasks:
                    break
                assert time.monotonic() < deadline and not future.done()
                time.sleep(.025)
            assert not any(row['record']['kind'] == 'review_reply' for row in history)
            task, = tasks
            bob('submit', task=task['id'], version=task['version'], answer=str(gate))
            assert future.result(timeout=8)['state'] == 'done'
        finally:
            gate.touch()
    assert lead('status', **run)['result']['outputs']['decision'][0]['answer'] == 'explained'
    records = [item['record'] for item in lead('history', **run)]
    assert next(i for i, r in enumerate(records) if r['kind'] == 'act' and r['node'] == 'independent') < next(
        i for i, r in enumerate(records) if r['kind'] == 'review_reply')


def test_nested_challenges_have_independent_budgets_and_archived_replies(service):
    db, lead, _, _ = service
    inner = {'nodes': [{'id': 'quote', 'kind': 'pass'}, {'id': 'verify', 'kind': 'function',
                       'function': __name__ + '.ask_for_price'}],
             'inputs': {'in': 'quote.x'}, 'outputs': {'out': 'verify.out'},
             'edges': [{'from': 'quote.y', 'to': 'verify.x', 'challenges': 1}]}
    graph = {'method': 'fan', 'nodes': [{'id': n, 'kind': 'block', 'graph': inner} for n in ('a', 'b')]}
    for bad in (False, -1, '2'):
        inner['edges'][0]['challenges'] = bad
        with pytest.raises(ValueError, match=r'edges\[0\].challenges'):
            lead('start', graph=graph)
    inner['edges'][0]['challenges'] = 1
    run = lead('start', graph=graph, inputs={'in': 9})
    assert work_one(db, 'groups', allow=[__name__])['state'] == 'done', lead('status', **run)
    for name in ('a', 'b'):
        history = lead('history', **run, scope=[[name, 1]])
        assert all(row['archived'] for row in history)
        q, reply = [row['record'] for row in history if row['record']['kind'] in ('challenge', 'review_reply')]
        assert q['version'] == 1 and q['id'] == reply['id'] and reply['answer'] == 'explained'


def test_fork_output_reopens_only_dependent_human_review(service):
    db, lead, alice, _ = service
    graph = {'nodes': [{'id': name, 'kind': 'human', 'outputs': [{'name': 'answer', 'schema': 'int'}]}
                       for name in ('price', 'review')],
             'edges': [{'from': 'price.answer', 'to': 'review.request'}],
             'outputs': {'signed': 'review.answer'}, 'inputs': {'x': 'price.request'}}
    run = lead('start', graph=graph, inputs={'x': 'quote'}, assignments={'price': 'Alice', 'review': 'Alice'})
    for answer in (9, 10):
        assert work_one(db, 'ask')['state'] == 'waiting'
        task, = [t for t in alice('tasks') if t['state'] == 'claimed']
        alice('submit', task=task['id'], version=task['version'], answer=answer)
    assert work_one(db, 'finish')['state'] == 'done'
    history = lead('history', **run)
    edit = {'node': 'price', 'token': 'answer#1', 'version': 1, 'value': 12, 'reason': 'revised quote'}
    branch = lead('fork', **run, at=history[-1]['seq'], output=edit, execute=True)['run']
    correction = lead('history', run=branch)[-1]['record']
    assert correction['external'] == {'actor': 'lead', 'reason': 'revised quote', 'previous_version': 1}
    assert work_one(db, 'ask-again')['state'] == 'waiting'
    pending, = [t for t in alice('tasks') if t['run'] == branch]
    assert pending['node'] == 'review' and pending['request'] == 12
    detail = alice('review', task=pending['id'])
    change, = detail['changes']
    assert change['previous'] == 9 and change['external']['reason'] == 'revised quote'
    assert change['external']['actor'] == 'lead' and detail['task']['request'] == 12
    assert 'impact' not in detail
    assert lead('review', task=pending['id'])['impact']['activated'] == ['review']
    bob = service[3]
    with pytest.raises(Refused):
        bob('review', task=pending['id'])
    moved = lead('assign', task=pending['id'], version=pending['version'], member='Bob')
    private_change, = bob('review', task=pending['id'])['changes']
    assert not private_change['can_compare'] and private_change['previous'] is None
    bob('submit', task=pending['id'], version=moved['version'], answer=13)
    assert work_one(db, 'finish-again')['state'] == 'done'
    assert lead('status', run=branch)['result']['outputs']['signed'] == [13]
    assert lead('history', **run) == history and lead('status', **run)['result']['outputs']['signed'] == [10]


def report_then_wait(ctx, gate):
    import time
    from pathlib import Path
    ctx.progress('reviewing evidence')
    deadline = time.monotonic() + 8
    while not Path(gate).exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('test reader did not release the gate')
        time.sleep(.02)
    return 'private result'


def test_live_progress_before_completion_and_cursor_resume(service, tmp_path, monkeypatch):
    from interlacing.collab.client import Client, ServiceError
    db, *_ = service
    monkeypatch.setenv('no_proxy', '127.0.0.1')
    service_api = Collaboration(db)
    token = service_api.bootstrap(str(uuid4()), 'lead')['token']
    graph = {'method': 'chain', 'nodes': [{'id': 'slow', 'kind': 'function', 'context': True,
                                           'function': __name__ + '.report_then_wait'}]}
    gate = tmp_path / 'release'
    with server(db, 0) as http, ThreadPoolExecutor(2) as pool:
        serving = pool.submit(http.serve_forever)
        try:
            url = 'http://127.0.0.1:' + str(http.server_port)
            client = Client(url, token)
            member = Client(url, client.request('member', name='Bob')['token'])
            run = client.request('start', graph=graph, inputs={'in': str(gate)})
            with pytest.raises(ServiceError) as err:
                next(member.stream_progress(**run))
            assert err.value.status == 403
            live = client.stream_progress(**run)
            assert next(live)['state'] == 'queued'
            worker = pool.submit(work_one, db, 'worker', allow=[__name__])
            observed = []
            for frame in live:
                if frame['type'] == 'event':
                    observed.append(frame['seq'])
                if frame.get('message') == 'reviewing evidence':
                    break
            assert not worker.done() and frame['node'] == 'slow'
            second = client.stream_progress(**run, after=observed[-1])
            assert next(second)['state'] == 'running'
            live.close()
            gate.touch()
            remaining = list(second)
            assert worker.result()['state'] == 'done' and remaining[-1]['state'] == 'done'
            assert observed + [f['seq'] for f in remaining if f['type']=='event'] == [
                e['seq'] for e in client.request('history', **run)]
            assert 'private result' not in str(remaining)
            with pytest.raises(ServiceError, match='cursor'):
                next(client.stream_progress(**run, after=9999))
        finally:
            gate.touch()
            http.shutdown()
            serving.result()


def test_concurrent_keyed_start_replays_one_receipt(service):
    db, lead, _, _ = service
    args = dict(graph=REVIEW, inputs={'x': 'quote'}, request_key='review-1')
    first, second = race(lambda: lead('start', **args), lambda: lead('start', **args))
    assert first and first == second == lead('start', **args)
    with pytest.raises(Refused, match='different start arguments'):
        lead('start', **{**args, 'workers': 0})
    assert [item['action'] for item in lead('audit')].count('run.start') == 1
    assert work_one(db, 'worker')['run'] == first['run']
    assert work_one(db, 'worker') is None


def test_invalid_start_does_not_consume_key(service):
    db, lead, _, _ = service
    with pytest.raises(Refused):
        lead('start', graph=REVIEW, assignments={'review': 'unknown'}, request_key='retry')
    run = lead('start', graph=REVIEW, inputs={'x': 'quote'}, request_key='retry')
    assert work_one(db, 'worker')['run'] == run['run']


def expert_probe():
    raise AssertionError("missing test probe")


def bounded_expert():
    """The test injects a counter shared by the worker's function nodes."""
    return expert_probe()


@pytest.mark.parametrize('workers,expected,kind', [(0, 1, 'block'), (3, 3, 'block'), (6, 6, 'graph')])
def test_nested_experts_share_one_run_budget(service, monkeypatch, workers, expected, kind):
    db, lead, _, _ = service
    lock = threading.Condition()
    active = peak = completed = 0
    def probe():
        nonlocal active, peak, completed
        with lock:
            active += 1
            peak = max(peak, active)
            lock.notify_all()
            try:
                assert lock.wait_for(lambda: peak >= expected, timeout=8)
                lock.wait(0.04)
                completed += 1
                return 1
            finally:
                active -= 1
    monkeypatch.setattr(sys.modules[__name__], 'expert_probe', probe)
    experts = {'nodes': [{'id': n, 'kind': 'function', 'function': __name__ + '.bounded_expert'}
                         for n in ('first', 'second')], 'outputs': {'result': 'second.out'}}
    graph = {'nodes': [{'id': group, 'kind': kind, 'graph': experts} for group in ('a', 'b', 'c')]}
    run = lead('start', graph=graph, workers=workers)
    assert work_one(db, 'worker', allow=[__name__])['state'] == 'done', lead('status', **run)
    assert (completed, peak) == (6, expected)
