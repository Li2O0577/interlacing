"""The command line and the run directory behind it."""
import io
import json
import subprocess
from pathlib import Path
import sys

import pytest

from interlacing import cli
from interlacing.runs import Run, models_from_environment
from interlacing.spec import SpecError

HERE = __name__
CALLS = []


def survey(x):
    CALLS.append('survey')
    return f'surveyed {x}'


def lead(ctx, task=None, reply=None):
    if task is not None:
        ctx.emit('send', task)
    else:
        verdict = ctx.call('approve', {'result': reply})
        ctx.emit('out', f"{reply}: {'released' if verdict['approved'] else 'held'}")


def fragile(x):
    if not FIXED:
        raise OSError('not yet')
    return x + 1


FIXED = []


def node(id, function, **more):
    return {'id': id, 'kind': 'function', 'function': f'{HERE}.{function}', 'inputs': ['x'], 'outputs': ['y'], **more}


GOVERNED = {'method': 'governed', 'max_rounds': 2,
            'root': {'id': 'lead', 'kind': 'function', 'function': f'{HERE}.lead', 'context': True, 'firing': 'any',
                     'inputs': ['task', {'name': 'reply', 'required': False}], 'outputs': ['out', 'send', {'name': 'approve', 'call': True}]},
            'children': [node('survey', 'survey')], 'operator': {'id': 'operator', 'kind': 'human'}}


def call(*argv):
    out = io.StringIO()
    code = cli.main([str(a) for a in argv], out)
    return code, json.loads(out.getvalue())


def write(path, description):
    path.write_text(json.dumps(description), encoding='utf-8')
    return path


def test_check_describes_a_graph_without_running_it(tmp_path):
    graph = write(tmp_path / 'g.json', {'method': 'chain', 'nodes': [node('a', 'survey'), {'id': 'w', 'kind': 'agent', 'model': 'anthropic:some-model'}]})
    CALLS.clear()
    code, said = call('check', graph, '--allow', HERE)
    assert code == 0 and said['nodes'] == {'a': 'function', 'w': 'agent'} and said['edges'] == 1 and len(said['digest']) == 64
    assert said['inputs'] == ['in'] and said['outputs'] == ['out'] and CALLS == []        # no key was needed, nothing ran
    code, said = call('check', graph)
    assert code == 1 and said['status'] == 'refused' and 'not in a module this catalog allows' in said['error']
    assert call('check', tmp_path / 'missing.json')[1]['error'].startswith('no such file')


def test_a_run_stops_for_a_person_and_goes_on_once_they_have_answered(tmp_path):
    graph, run = write(tmp_path / 'g.json', GOVERNED), tmp_path / 'run'
    CALLS.clear()
    code, said = call('run', graph, '--dir', run, '--input', 'in=the bridge', '--allow', HERE)
    assert code == 3 and said['status'] == 'waiting'
    assert said['questions'] == [{'id': 'operator-1', 'node': 'operator', 'round': 1, 'request': {'result': 'surveyed the bridge'}}]
    assert call('status', '--dir', run)[1]['status'] == 'waiting'
    code, said = call('resume', '--dir', run)                      # nobody has answered: it waits again, and asks nothing new
    assert code == 3 and [q['id'] for q in said['questions']] == ['operator-1']
    code, said = call('answer', '--dir', run, '--json', '{"approved": true}')
    assert code == 0 and said['questions'] == []
    code, said = call('resume', '--dir', run)
    assert code == 0 and said['status'] == 'done' and said['outputs'] == {'out': ['surveyed the bridge: released']}
    assert CALLS == ['survey'] and said['rounds'] == {'lead': 2, 'survey': 1, 'operator': 1}
    assert call('resume', '--dir', run)[1]['outputs'] == {'out': ['surveyed the bridge: released']}     # done stays done
    assert sorted(p.name for p in run.iterdir()) == ['answers', 'files', 'graph.json', 'inputs.json', 'journal.jsonl', 'options.json', 'questions', 'state.json']


def test_a_failed_run_says_where_and_can_be_resumed(tmp_path):
    graph, run = write(tmp_path / 'g.json', {'method': 'chain', 'nodes': [node('a', 'fragile')]}), tmp_path / 'run'
    FIXED.clear()
    code, said = call('run', graph, '--dir', run, '--input', 'in=1', '--allow', HERE)
    assert code == 1 and said['status'] == 'failed' and said['node'] == 'a' and 'not yet' in said['error']
    FIXED.append(True)
    code, said = call('resume', '--dir', run)
    assert code == 0 and said['outputs'] == {'out': [2]}


def test_what_the_command_line_refuses(tmp_path):
    graph, run = write(tmp_path / 'g.json', {'method': 'chain', 'nodes': [node('a', 'survey')]}), tmp_path / 'run'
    assert call('resume', '--dir', run)[1]['error'].endswith('holds no run')
    assert call('status', '--dir', run)[1]['error'].endswith('holds no run')
    assert 'NAME=VALUE' in call('run', graph, '--dir', run, '--input', 'oops', '--allow', HERE)[1]['error']
    assert call('run', graph, '--dir', run, '--inputs', write(tmp_path / 'in.json', {'in': 'x'}), '--allow', HERE)[0] == 0
    assert 'already holds a run' in call('run', graph, '--dir', run, '--allow', HERE)[1]['error']
    assert call('answer', '--dir', run, '--text', 'hello')[1]['error'] == 'no question is open'
    assert call('answer', '--dir', run, 'operator-9', '--text', 'hello')[1]['error'] == "no open question 'operator-9'"
    blocked = write(tmp_path / 'p.json', {'nodes': [{'id': 'p', 'kind': 'process', 'command': ['ls']}]})
    assert 'does not allow processes' in call('run', blocked, '--dir', tmp_path / 'r2')[1]['error']
    assert not (tmp_path / 'r2' / 'graph.json').exists()
    with pytest.raises(SpecError, match='has already been started'):
        Run(run).start()


def test_models_come_from_the_environment_and_keys_stay_out_of_the_run(tmp_path, monkeypatch):
    monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
    with pytest.raises(SpecError, match="needs ANTHROPIC_API_KEY in the environment"):
        models_from_environment('anthropic:some-model')
    with pytest.raises(SpecError, match='known providers: anthropic, openai, fake'):
        models_from_environment('nowhere:x')
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'secret-key-value')
    monkeypatch.setenv('OPENAI_API_KEY', 'other-secret')
    monkeypatch.setenv('ANTHROPIC_BASE_URL', 'http://127.0.0.1:9')       # nothing listens there: no service is called
    monkeypatch.setattr('interlacing.models.providers.RETRY_WAITS', (0,))
    assert models_from_environment('anthropic:some-model').name == 'anthropic:some-model'
    assert models_from_environment('openai:some-model').url == 'https://api.openai.com/v1/chat/completions'
    graph, run = write(tmp_path / 'g.json', {'method': 'chain', 'nodes': [{'id': 'w', 'kind': 'agent', 'model': 'fake:echo'},
                                                                         {'id': 'k', 'kind': 'agent', 'model': 'anthropic:some-model', 'max_steps': 1}]}), tmp_path / 'run'
    code, said = call('run', graph, '--dir', run, '--input', 'in=hello')
    assert said['status'] == 'failed' and said['node'] == 'k'            # the echo ran; the other has no service to reach
    everything = ''.join(p.read_text(encoding='utf-8') for p in run.rglob('*') if p.is_file())
    assert 'secret-key-value' not in everything and 'other-secret' not in everything and 'hello' in everything


def test_the_installed_command_runs(tmp_path):
    graph = write(tmp_path / 'g.json', {'method': 'chain', 'nodes': [{'id': 'w', 'kind': 'agent', 'model': 'fake:echo'}]})
    done = subprocess.run([sys.executable, '-m', 'interlacing.cli', 'run', str(graph), '--dir', str(tmp_path / 'run'), '--input', 'in=你好'],
                          capture_output=True, text=True, encoding='utf-8')
    assert done.returncode == 0 and json.loads(done.stdout)['outputs'] == {'out': ['你好']}
    assert subprocess.run([sys.executable, '-m', 'interlacing.cli', '--version'], capture_output=True, text=True).stdout.strip()


def test_guide_prints_the_format_and_the_allowed_functions():
    out = io.StringIO()
    assert cli.main(['guide', '--allow', HERE], out) == 0
    assert out.getvalue().startswith('A graph is JSON.') and f'- {HERE}.' in out.getvalue()


def test_a_module_beside_where_the_command_is_run_can_be_allowed(tmp_path, monkeypatch):
    (tmp_path / 'besidemod.py').write_text('def up(text):\n    return text.upper()\n', encoding='utf-8')
    write(tmp_path / 'g.json', {'method': 'chain', 'nodes': [{'id': 'u', 'kind': 'function', 'function': 'besidemod.up'}]})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, 'path', [p for p in sys.path if p not in ('', str(tmp_path))])
    code, state = call('run', tmp_path / 'g.json', '--dir', tmp_path / 'r', '--allow', 'besidemod', '--input', 'in=hi')
    assert code == 0 and state['outputs'] == {'out': ['HI']}


def test_a_reader_that_goes_away_is_not_an_error(tmp_path):
    import os
    write(tmp_path / 'g.json', {'method': 'chain', 'nodes': [{'id': 'u', 'kind': 'function', 'function': f'{HERE}.shout'}]})
    child = subprocess.Popen([sys.executable, '-c', 'import sys; from interlacing.cli import main; sys.exit(main())',
                              'check', str(tmp_path / 'g.json'), '--allow', HERE],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=str(tmp_path),
                             env={**os.environ, 'PYTHONPATH': os.path.dirname(os.path.abspath(__file__))})
    child.stdout.close()                      # the reader is gone before anything is written
    error = child.stderr.read().decode()
    assert child.wait(60) == 0 and error == ''


def shout(text):
    return text.upper()


def test_show_explains_first_delivery_without_loading_nodes_or_repairing_journal(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root / 'examples/data')
    run = tmp_path / 'run'
    assert call('run', 'release.json', '--allow', 'steps', '--dir', run, '--input', 'topic=tides')[0] == 3
    path = run / 'journal.jsonl'
    original = path.read_bytes()
    path.write_bytes(original + b'{"kind":')
    with monkeypatch.context() as context:
        context.setattr(Run, '_engine', lambda self: pytest.fail('inspection constructed nodes'))
        code, view = call('show', '--dir', run)
    assert code == 0 and view['nodes']['editor']['questions'] == ['editor-1']
    publish = view['nodes']['publish']
    assert publish['awaiting_first_delivery_from'] == ['editor.answer']
    assert publish['inputs']['note'][0]['delivery_recorded']
    assert path.read_bytes() == original + b'{"kind":'
    path.write_bytes(original)
    assert call('answer', '--dir', run, '--json', '{"approved": true}')[0] == 0
    assert 'questions' not in call('show', '--dir', run)[1]['nodes']['editor']
    assert call('resume', '--dir', run)[0] == 0
    done = call('show', '--dir', run)[1]
    assert done['status'] == 'done' and done['nodes']['publish']['activation_recorded']
    assert 'awaiting_first_delivery_from' not in done['nodes']['publish']


def test_show_marks_changed_topology_and_reports_corrupt_complete_records(tmp_path):
    run = Run.create(tmp_path, {'method': 'chain', 'nodes': [node('a', 'survey')]}, allow=[HERE])
    assert run.show()['status'] == 'new'
    journal = tmp_path / 'journal.jsonl'
    journal.write_text('{"kind":"change"}\n', encoding='utf-8')
    assert run.show()['topology'].startswith('last known graph')
    journal.write_text('invalid\n', encoding='utf-8')
    assert call('show', '--dir', tmp_path)[0] == 1


def test_show_replays_dynamic_topology_and_formats(tmp_path, monkeypatch):
    import importlib
    example = Path(__file__).resolve().parents[1] / 'examples' / 'graphs'
    monkeypatch.syspath_prepend(str(example))
    module = importlib.import_module('changed_while_running')
    plan = {'id': 'plan', 'kind': 'function', 'function': module.__name__ + '.plan',
            'context': True, 'inputs': ['task'], 'outputs': ['p0', 'p1']}
    run = Run.create(tmp_path, {'nodes': [plan], 'inputs': {'in': 'plan.task'}}, allow=[module.__name__])
    assert run.start({'in': 'alpha, beta'})['status'] == 'done'
    view = run.show(timeline=True)
    assert set(view['nodes']) == {'plan', 'solve0', 'solve1'}
    assert view['topology'] == 'journaled graph' and view['outputs']['part1'] == 'solve1.y'
    assert any(e.get('operations', []).count('add') == 2 for e in view['timeline'])
    assert all(e['elapsed_seconds'] >= 0 for e in view['timeline'] if e['kind'] == 'act')
    for fmt, header in [('dot', 'digraph'), ('mermaid', 'flowchart')]:
        out = io.StringIO()
        assert cli.main(['show', '--dir', str(tmp_path), '--format', fmt], out) == 0
        assert out.getvalue().startswith(header) and 'p1 to x (value)' in out.getvalue()


def test_show_forgets_removed_members_and_their_delivery_evidence(tmp_path):
    from interlacing.inspection import explain
    def part(name):
        return {'id': name, 'firing': 'all', 'inputs': [{'name': 'x', 'required': True}], 'outputs': []}
    edge = {'id': 'e', 'from': 'a.y', 'to': 'b.x', 'protocol': 'value'}
    graph = {'nodes': [part('a'), part('b')], 'edges': [edge], 'inputs': {}, 'outputs': {'old': 'b.y'},
             'manages': {'a': ['b']}}
    path = tmp_path / 'journal'
    records = [{'kind': 'act', 'node': 'a', 'sent': [['e', 1]]},
               {'kind': 'act', 'node': 'a', 'changes': [[['remove', 'b'], ['add', part('c')],
                 ['connect', dict(edge, to='c.x')], ['manage', 'a', ['c']], ['output', 'new', 'c.y']]]}]
    path.write_text(''.join(json.dumps(r) + '\n' for r in records))
    view = explain(graph, path, 'waiting', [], timeline=True)
    assert set(view['nodes']) == {'a', 'c'} and view['outputs'] == {'new': 'c.y'}
    assert view['managers'] == {'c': 'a'}
    assert view['nodes']['c']['awaiting_first_delivery_from'] == ['a.y']
    assert all('elapsed_seconds' not in e for e in view['timeline'])


@pytest.mark.parametrize("cut_after_correction", [False, True])
def test_resumed_cooling_review_replaces_the_existing_report(tmp_path, monkeypatch, cut_after_correction):
    data = Path(__file__).resolve().parents[1] / 'examples' / 'data'
    monkeypatch.syspath_prepend(str(data))
    run = Run.create(tmp_path / 'simulation', json.loads((data / 'cooling_review.json').read_text()), allow=['steps'])
    waiting = run.start({'steps': 100})
    assert waiting['status'] == 'waiting' and waiting['questions'][0]['request'] == 36.6032
    assert run.show()['nodes']['b2']['activation_recorded']
    run.answer({'units': 'percent', 'reading': waiting['questions'][0]['request']})
    if cut_after_correction:
        # Kill a separate process exactly after A durably corrected the input, before B2 runs again.
        script = """
import os, sys
from interlacing import Journal
from interlacing.runs import Run
write = Journal.write
def cut(self, record):
    write(self, record)
    if record.get('redos'):
        os._exit(37)
Journal.write = cut
Run(sys.argv[1]).resume()
"""
        process = subprocess.run([sys.executable, '-c', script, str(run.directory)], cwd=data, timeout=20)
        assert process.returncode == 37
    done = Run(run.directory).resume()
    assert done['status'] == 'done' and done['outputs'] == {'report': [0.366]}
    assert done['used'] == {'simulations': 1} and done['rounds'] == {'a': 2, 'b1': 1, 'b2': 2}
    timeline = run.show(timeline=True)['timeline']
    assert [event['revised_outputs'] for event in timeline if 'revised_outputs' in event] == [
        [{'output': 'report', 'index': 0}]]
