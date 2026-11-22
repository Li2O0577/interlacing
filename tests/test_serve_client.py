"""The server and a real client: the reference client of the Model Context Protocol, from the ``mcp`` package,
starts the server as a program and speaks to it over stdio. The package is a tool for this test only; where
it is not installed the test is skipped."""
import asyncio
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip('mcp')

from mcp import ClientSession, StdioServerParameters      # noqa: E402
from mcp.client.stdio import stdio_client                  # noqa: E402

HERE = __name__


def survey(x):
    print('a node may print')
    return f'surveyed {x}'


def slow(ctx, x):
    import time
    while not ctx.stopping():
        time.sleep(0.01)


def of(thing, *names):
    """A field of what the client returned, under whichever spelling this version of the client uses."""
    for name in names:
        if hasattr(thing, name):
            return getattr(thing, name)
    raise AttributeError(names)


def told(result):
    return of(result, 'structuredContent', 'structured_content')


def failed(result):
    return of(result, 'isError', 'is_error')


REVIEWED = {'nodes': [{'id': 'survey', 'kind': 'function', 'function': f'{HERE}.survey', 'inputs': ['x'], 'outputs': ['y']},
                      {'id': 'editor', 'kind': 'human'}],
            'edges': [{'from': 'survey.y', 'to': 'editor.request'}],
            'inputs': {'site': 'survey.x'}, 'outputs': {'verdict': 'editor.answer'}}
SLOW = {'method': 'chain', 'nodes': [{'id': 'slow', 'kind': 'function', 'function': f'{HERE}.slow', 'context': True,
                                      'inputs': ['x'], 'outputs': ['y']}]}


def test_the_reference_client_runs_a_graph_through_the_server(tmp_path):
    server = StdioServerParameters(
        command=sys.executable, args=['-m', 'interlacing.cli', 'serve', '--runs', str(tmp_path), '--allow', HERE],
        env={**os.environ, 'PYTHONPATH': os.pathsep.join([str(Path(__file__).parent), os.environ.get('PYTHONPATH', '')])})

    async def main():
        async with stdio_client(server) as (read, write), ClientSession(read, write) as session:
            hello = await session.initialize()
            assert of(hello, 'serverInfo', 'server_info').name == 'interlacing' and hello.capabilities.tools is not None
            tools = {tool.name: tool for tool in (await session.list_tools()).tools}
            assert sorted(tools) == ['answer', 'check', 'control', 'guide', 'resume', 'runs', 'start', 'status', 'stop']
            assert of(tools['start'], 'inputSchema', 'input_schema')['required'] == ['graph']

            checked = await session.call_tool('check', {'graph': REVIEWED})
            assert not failed(checked) and told(checked)['nodes'] == {'survey': 'function', 'editor': 'human'}

            started = await session.call_tool('start', {'graph': REVIEWED, 'inputs': {'site': '桥'}})
            state = told(started)
            assert not failed(started) and state['status'] == 'waiting' and state['questions'][0]['request'] == 'surveyed 桥'
            await session.call_tool('answer', {'run': state['run'], 'answer': {'approved': True}})
            resumed = await session.call_tool('resume', {'run': state['run']})
            assert told(resumed)['status'] == 'done'
            assert told(resumed)['outputs'] == {'verdict': [{'approved': True}]}

            # a run that goes on behind its call, asked about and stopped while another call is answered
            behind = told(await session.call_tool('start', {'graph': SLOW, 'inputs': {'in': 1}, 'wait': False}))
            assert behind['status'] == 'running'
            listed = told(await session.call_tool('runs', {}))['runs']
            assert {run['run']: run['status'] for run in listed} == {behind['run']: 'running', state['run']: 'done'}
            await session.call_tool('stop', {'run': behind['run']})
            for _ in range(500):
                if told(await session.call_tool('status', {'run': behind['run']}))['status'] == 'stopped':
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError('the run did not stop')

            refused = await session.call_tool('status', {'run': '../outside'})
            assert failed(refused) and told(refused)['status'] == 'refused'
    asyncio.run(asyncio.wait_for(main(), 60))
