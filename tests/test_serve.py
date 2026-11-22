"""The MCP server: the command line's operations, offered to a client that the server does not trust."""
import io
import json
import subprocess
import sys
import threading
import time

from interlacing import cli
from interlacing.serve import TOOLS, Server

HERE = __name__
CALLS = []


def survey(x):
    CALLS.append('survey')
    print('a node may print')                                       # must not reach the client
    return f'surveyed {x}'


def broken(x):
    raise OSError('the instrument is off')


GATES = {'entered': threading.Event(), 'open': threading.Event()}


def gate(x):
    """Waits until the test lets it through."""
    GATES['entered'].set()
    if not GATES['open'].wait(10):
        raise TimeoutError('nobody opened the gate')
    return f'through {x}'


def patient(ctx, x):
    """Works until the run is told to stop."""
    GATES['entered'].set()
    while not ctx.stopping():
        time.sleep(0.005)


def node(id, function):
    return {'id': id, 'kind': 'function', 'function': f'{HERE}.{function}', 'inputs': ['x'], 'outputs': ['y']}


REVIEWED = {'nodes': [node('survey', 'survey'), {'id': 'editor', 'kind': 'human'}],
            'edges': [{'from': 'survey.y', 'to': 'editor.request'}],
            'inputs': {'site': 'survey.x'}, 'outputs': {'verdict': 'editor.answer'}}


def tool(server, name, **arguments):
    reply = server.handle({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {'name': name, 'arguments': arguments}})
    result = reply['result']
    assert json.loads(result['content'][0]['text']) == result['structuredContent']
    return result['isError'], result['structuredContent']


def test_the_handshake_and_the_tool_list(tmp_path):
    server = Server(tmp_path)
    hello = server.handle({'jsonrpc': '2.0', 'id': 0, 'method': 'initialize', 'params': {'protocolVersion': '2025-03-26', 'capabilities': {}}})
    assert hello['result']['protocolVersion'] == '2025-03-26' and hello['result']['capabilities'] == {'tools': {}}
    assert hello['result']['serverInfo']['name'] == 'interlacing'
    assert server.handle({'jsonrpc': '2.0', 'id': 0, 'method': 'initialize', 'params': {'protocolVersion': '1999-01-01'}})['result']['protocolVersion'] == '2025-06-18'
    assert server.handle({'jsonrpc': '2.0', 'method': 'notifications/initialized'}) is None
    assert server.handle({'jsonrpc': '2.0', 'id': 2, 'method': 'ping'})['result'] == {}
    listed = server.handle({'jsonrpc': '2.0', 'id': 3, 'method': 'tools/list'})['result']['tools']
    assert [t['name'] for t in listed] == ['guide', 'check', 'start', 'status', 'resume', 'stop', 'control', 'answer', 'runs'] and listed == TOOLS
    assert all(t['inputSchema']['type'] == 'object' and t['description'] for t in listed)
    assert server.handle({'jsonrpc': '2.0', 'id': 4, 'method': 'resources/list'})['error']['code'] == -32601
    assert server.handle({'jsonrpc': '2.0', 'id': 5, 'method': 'tools/call', 'params': {'name': 'erase'}})['error']['code'] == -32602
    assert server.handle([1, 2])['error']['code'] == -32600


def test_a_client_runs_a_graph_and_a_person_answers(tmp_path):
    server = Server(tmp_path, allow=[HERE])
    CALLS.clear()
    failed, said = tool(server, 'check', graph=REVIEWED)
    assert not failed and said['nodes'] == {'survey': 'function', 'editor': 'human'} and CALLS == []
    failed, said = tool(server, 'start', graph=REVIEWED, inputs={'site': 'the bridge'})
    run = said['run']
    assert not failed and said['status'] == 'waiting' and said['questions'][0]['request'] == 'surveyed the bridge'
    assert tool(server, 'status', run=run)[1]['status'] == 'waiting'
    assert tool(server, 'runs')[1]['runs'][0]['run'] == run
    failed, said = tool(server, 'answer', run=run, answer={'approved': True})
    assert not failed and said['questions'] == []
    failed, said = tool(server, 'resume', run=run)
    assert not failed and said['status'] == 'done' and said['outputs'] == {'verdict': [{'approved': True}]} and CALLS == ['survey']
    assert (tmp_path / run / 'journal.jsonl').exists()             # the same directory the command line works on
    out = io.StringIO()
    assert cli.main(['status', '--dir', str(tmp_path / run)], out) == 0 and json.loads(out.getvalue())['status'] == 'done'


def test_the_client_cannot_reach_past_what_the_server_was_given(tmp_path):
    server = Server(tmp_path / 'runs')
    failed, said = tool(server, 'start', graph=REVIEWED, inputs={'site': 'x'})
    assert failed and said['status'] == 'refused' and 'not in a module this catalog allows' in said['error']
    failed, said = tool(server, 'start', graph={'nodes': [{'id': 'p', 'kind': 'process', 'command': ['ls']}]})
    assert failed and 'does not allow processes' in said['error']
    assert tool(server, 'start', graph=REVIEWED, allow=[HERE], processes=True)[0]       # asking for more changes nothing
    assert tool(server, 'runs')[1] == {'runs': []} and not list((tmp_path / 'runs').glob('*/graph.json'))
    for name in ('../outside', '/etc', '', 'nope', 7):
        failed, said = tool(server, 'status', run=name)
        assert failed and said['error'] == f'no run {name!r}'
    assert tool(server, 'check', graph='not a graph')[1]['error'].startswith('graph must be an object')
    assert tool(server, 'start', graph=REVIEWED, inputs=[1])[1]['error'].startswith('inputs must be an object')


def test_a_failing_node_is_reported_and_the_server_goes_on(tmp_path):
    server = Server(tmp_path, allow=[HERE])
    failed, said = tool(server, 'start', graph={'method': 'chain', 'nodes': [node('a', 'broken')]}, inputs={'in': 1})
    assert not failed and said['status'] == 'failed' and said['node'] == 'a' and 'the instrument is off' in said['error']
    assert tool(server, 'answer', run=said['run'], answer=1)[1]['error'] == 'no question is open'
    assert tool(server, 'runs')[1]['runs'][0]['status'] == 'failed'


def test_the_server_speaks_over_stdio_and_only_the_protocol_reaches_the_client(tmp_path):
    messages = [{'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2025-06-18', 'capabilities': {}}},
                {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
                {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {'name': 'start', 'arguments': {'graph': REVIEWED, 'inputs': {'site': '桥'}}}}]
    sent = '\n'.join(json.dumps(m) for m in messages) + '\nthis is not json\n\n'
    done = subprocess.run([sys.executable, '-m', 'interlacing.cli', 'serve', '--runs', str(tmp_path), '--allow', HERE],
                          input=sent, capture_output=True, text=True, encoding='utf-8', cwd=str(tmp_path),
                          env={**__import__('os').environ, 'PYTHONPATH': str(__import__('pathlib').Path(__file__).parent)})
    replies = [json.loads(line) for line in done.stdout.splitlines()]
    assert done.returncode == 0 and replies[0]['id'] == 1 and sorted(str(r.get('id')) for r in replies) == ['1', '2', 'None']
    by_id = {r.get('id'): r for r in replies}                       # a tool call is answered when it is done
    assert by_id[2]['result']['structuredContent']['questions'][0]['request'] == 'surveyed 桥'
    assert by_id[None]['error']['code'] == -32700 and 'a node may print' in done.stderr


# ---- side by side ---------------------------------------------------------------------------------------

GATED = {'method': 'chain', 'nodes': [node('gate', 'gate')]}
PATIENT = {'method': 'chain', 'nodes': [{**node('patient', 'patient'), 'context': True}]}


def gates():
    for event in GATES.values():
        event.clear()


def until(condition, seconds=10):
    end = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < end, 'it did not happen'
        time.sleep(0.005)


class Client:
    """Speaks to a server over a pair of pipes, as a client does: it writes when it likes and reads replies as they come."""

    def __init__(self, server):
        self.replies, self.lines, self.more = {}, [], threading.Condition()
        self.closed, self.id = False, 0
        self.thread = threading.Thread(target=server.serve, args=(self._read(), self), daemon=True)
        self.thread.start()

    def _read(self):
        while True:
            with self.more:
                while not self.lines and not self.closed:
                    self.more.wait()
                if not self.lines:
                    return
                line = self.lines.pop(0)
            yield line

    def write(self, text):                                           # what the server sends
        reply = json.loads(text)
        with self.more:
            self.replies[reply.get('id')] = reply
            self.more.notify_all()

    def flush(self):
        pass

    def send(self, method, **params):
        self.id += 1
        with self.more:
            self.lines.append(json.dumps({'jsonrpc': '2.0', 'id': self.id, 'method': method, 'params': params}))
            self.more.notify_all()
        return self.id

    def notify(self, method, **params):
        with self.more:
            self.lines.append(json.dumps({'jsonrpc': '2.0', 'method': method, 'params': params}))
            self.more.notify_all()

    def call(self, name, **arguments):
        return self.send('tools/call', name=name, arguments=arguments)

    def reply(self, id, seconds=10):
        with self.more:
            assert self.more.wait_for(lambda: id in self.replies, seconds), f'no reply to {id}'
            return self.replies[id]['result']['structuredContent']

    def close(self):
        with self.more:
            self.closed = True
            self.more.notify_all()
        self.thread.join(10)
        assert not self.thread.is_alive()


def test_a_long_run_does_not_hold_up_other_calls(tmp_path):
    gates()
    client = Client(Server(tmp_path, allow=[HERE]))
    long = client.call('start', graph=GATED, inputs={'in': 'a'})
    until(GATES['entered'].is_set)
    listed = client.reply(client.call('runs'))['runs']              # answered while the first call is still going
    assert [r['status'] for r in listed] == ['running'] and long not in client.replies
    assert client.reply(client.call('status', run=listed[0]['run']))['status'] == 'running'
    refused = client.reply(client.call('resume', run=listed[0]['run']))
    assert refused['status'] == 'refused' and 'already going' in refused['error']
    GATES['open'].set()
    done = client.reply(long)
    assert done['status'] == 'done' and done['outputs'] == {'out': ['through a']}
    client.close()


def test_a_run_goes_on_behind_its_call_and_is_asked_about(tmp_path):
    gates()
    client = Client(Server(tmp_path, allow=[HERE]))
    started = client.reply(client.call('start', graph=GATED, inputs={'in': 'b'}, wait=False))
    assert started['status'] == 'running' and GATES['entered'].wait(10)
    run = started['run']
    assert client.reply(client.call('status', run=run))['status'] == 'running'
    GATES['open'].set()
    until(lambda: client.reply(client.call('status', run=run))['status'] == 'done')
    assert client.reply(client.call('status', run=run))['outputs'] == {'out': ['through b']}
    client.close()


def test_a_run_is_stopped_and_taken_up_again(tmp_path):
    gates()
    client = Client(Server(tmp_path, allow=[HERE]))
    run = client.reply(client.call('start', graph=PATIENT, inputs={'in': 1}, wait=False))['run']
    until(GATES['entered'].is_set)
    assert client.reply(client.call('stop', run=run))['status'] == 'stopping'
    until(lambda: client.reply(client.call('status', run=run))['status'] == 'stopped')
    idle = client.reply(client.call('stop', run=run))
    assert idle['status'] == 'refused' and 'not going' in idle['error']
    gates()
    again = client.call('resume', run=run)                           # it runs again from where it stopped
    until(GATES['entered'].is_set)
    client.reply(client.call('stop', run=run))
    assert client.reply(again)['status'] == 'stopped'
    client.close()


def test_a_cancelled_call_stops_its_run(tmp_path):
    gates()
    client = Client(Server(tmp_path, allow=[HERE]))
    call = client.call('start', graph=PATIENT, inputs={'in': 1})
    until(GATES['entered'].is_set)
    client.notify('notifications/cancelled', requestId=call, reason='the person lost patience')
    assert client.reply(call)['status'] == 'stopped'
    client.close()


def test_a_client_that_goes_away_leaves_nothing_running(tmp_path):
    gates()
    server = Server(tmp_path, allow=[HERE])
    client = Client(server)
    run = client.reply(client.call('start', graph=PATIENT, inputs={'in': 1}, wait=False))['run']
    until(GATES['entered'].is_set)
    client.close()
    assert tool(server, 'status', run=run)[1]['status'] == 'stopped' and not (tmp_path / run / 'lock').exists()


def test_the_guide_tool_gives_the_format_and_the_functions_a_graph_may_name(tmp_path):
    from interlacing.spec import GUIDE
    failed, state = tool(Server(tmp_path, allow=[HERE]), 'guide')
    assert not failed and state['guide'] == GUIDE
    assert f'{HERE}.survey' in {f['name'] for f in state['functions']}
