"""Historical SQLite views and independent run directories."""

import io
import json
import sqlite3

import pytest

from interlacing import Change, FunctionNode, Port, SQLiteJournal, cli
from interlacing.runs import Run
from interlacing.spec import SpecError


CALLS = []


def twice(x):
    CALLS.append(('twice', x))
    return x * 2


def grow(ctx, x):
    CALLS.append(('grow', x))
    added = FunctionNode('added', twice, inputs=[Port('x')], outputs=[Port('y')])
    ctx.change(Change().add(added).connect('grow.y', 'added.x').output('answer', 'added.y'))
    return x


GRAPH = {'nodes': [{'id': 'grow', 'kind': 'function', 'function': f'{__name__}.grow', 'context': True,
                    'inputs': [{'name': 'x', 'schema': 'int'}], 'outputs': ['y']}], 'inputs': {'number': 'grow.x'}}


def call(*argv):
    out = io.StringIO()
    code = cli.main([str(arg) for arg in argv], out)
    return code, json.loads(out.getvalue())


def test_historical_queries_and_two_executable_forks_from_one_checkpoint(tmp_path):
    CALLS.clear()
    source = Run.create(tmp_path / 'source', GRAPH, allow=[__name__], journal='sqlite', durable=True,
                        workspace=tmp_path / 'shared')
    assert source.start({'number': 4})['outputs'] == {'answer': [8]}
    journal = SQLiteJournal(source.directory / 'journal.sqlite3', durable=True)
    original = journal.read()
    status = source.status()
    source_view = source.show(timeline=True)
    initial = call('show', '--dir', source.directory, '--at', 1)[1]
    assert initial['forkable'] and initial['checkpoint'] == 1
    assert list(initial['nodes']) == ['grow'] and initial['values'] == {}
    changed_at = next(i for i, record in enumerate(original, 1) if record['kind'] == 'topology')
    changed = source.show(at=changed_at, timeline=True)
    assert changed['graph_version']['changes'] == 1 and changed['graph_version'] != initial['graph_version']
    assert changed['outputs'] == {'answer': 'added.y'} and changed['values'] == {}
    assert not changed['forkable'] and changed['pending']['grow']['took'] == ['input:number#1']
    assert changed['timeline'][-1]['seq'] == changed_at
    final = source.show(at=len(original))
    assert final['ended'] and final['values'] == {'answer': [8]}
    assert final['graph_version'] == changed['graph_version']
    assert call('checkpoints', '--dir', source.directory) == (0, {'last_event': len(original), 'checkpoints': [1, len(original)]})

    ids = {source_view['provenance']['run_id']}
    for number in (5, 7):
        destination = tmp_path / f'branch-{number}'
        assert call('fork', '--dir', source.directory, '--to', destination, '--at', 1,
                    '--input', f'number={number}', '--execute')[0] == 0
        branch = Run(destination)
        assert branch.workspace() == destination / 'files'
        assert branch.checkpoints() == {'last_event': 2, 'checkpoints': [2]}
        if number == 7:
            with sqlite3.connect(destination / 'journal.sqlite3') as connection:
                connection.execute('DELETE FROM checkpoints')
        assert call('resume', '--dir', destination)[1]['outputs'] == {'answer': [number * 2]}
        view = branch.show(timeline=True)
        assert view['fork'] == {'origin': {'run_id': source_view['provenance']['run_id'],
                                           'scope_id': source_view['provenance']['scope_id'], 'event': 1},
                                'executable': True}
        assert view['provenance']['run_id'] not in ids
        ids.add(view['provenance']['run_id'])
        read = view['provenance']['activations'][0]['reads'][0]
        assert read['delivery'] == 'input:number#2' and read['source']['replaces'] == 'input:number#1'
        assert read['source']['event_seq'] == 2
        assert branch.resume()['outputs'] == {'answer': [number * 2]}
    assert CALLS == [('grow', 4), ('twice', 4), ('grow', 5), ('twice', 5), ('grow', 7), ('twice', 7)]
    assert journal.read() == original and source.status() == status and source.show(timeline=True) == source_view


def test_later_fork_reuses_finished_work_and_enforces_boundaries(tmp_path):
    CALLS.clear()
    source = Run.create(tmp_path / 'source', GRAPH, allow=[__name__], journal='sqlite', durable=True)
    source.start({'number': 4})
    records = SQLiteJournal(source.directory / 'journal.sqlite3').read()
    midway = next(i for i, record in enumerate(records, 1) if record['kind'] == 'act' and record['node'] == 'grow')
    assert source.show(at=midway)['forkable']
    destination = tmp_path / 'branch'
    with pytest.raises(SpecError, match='unfinished activations'):
        source.fork(destination, at=midway - 1)
    with pytest.raises(SpecError, match='no longer available'):
        source.fork(destination, at=midway, inputs={'number': 8})
    with pytest.raises(SpecError, match='graph input'):
        source.fork(destination, at=1, inputs={'number': 'bad'})
    assert not destination.exists()
    assert call('show', '--dir', source.directory, '--at', len(records) + 1)[0] == 1

    inspect_only = source.fork(destination, at=midway)
    assert inspect_only.show()['fork']['executable'] is False
    with pytest.raises(SpecError, match='for inspection'):
        inspect_only.resume()
    with pytest.raises(SpecError, match='for inspection'):
        inspect_only.control('added', 'pause')
    with pytest.raises(SpecError, match='already exists'):
        source.fork(destination, at=1, execute=True)
    assert CALLS == [('grow', 4), ('twice', 4)]

    executable = source.fork(tmp_path / 'execute', at=midway, execute=True)
    assert executable.resume()['outputs'] == {'answer': [8]}
    assert CALLS == [('grow', 4), ('twice', 4), ('twice', 4)]
    ended = source.fork(tmp_path / 'ended', at=len(records), execute=True)
    assert ended.show(at=len(records) + 1)['ended']
    assert ended.resume()['outputs'] == {'answer': [8]}
    assert CALLS == [('grow', 4), ('twice', 4), ('twice', 4)]


def test_fork_of_a_fork_can_take_a_position_before_the_first_override(tmp_path):
    source = Run.create(tmp_path / 'source', GRAPH, allow=[__name__], journal='sqlite', durable=True)
    source.start({'number': 4})
    first = source.fork(tmp_path / 'first', at=1, inputs={'number': 9})
    grandchild = first.fork(tmp_path / 'grandchild', at=1, execute=True)
    assert grandchild.show()['fork']['origin']['run_id'] == first.show(timeline=True)['provenance']['run_id']
    assert grandchild.resume()['outputs'] == {'answer': [8]}
    assert json.loads((grandchild.directory / 'inputs.json').read_text()) == {'number': 4}
    after = first.fork(tmp_path / 'after', at=2, inputs={'number': 11}, execute=True)
    assert after.resume()['outputs'] == {'answer': [22]}
    assert [event['kind'] for event in after.show(timeline=True)['timeline'][:3]] == ['run', 'fork', 'fork']


def test_v2_checkpoints_are_read_without_migration_then_retained_when_written(tmp_path):
    path = tmp_path / 'legacy.sqlite3'
    journal = SQLiteJournal(path)
    journal.write({'value': 1})
    journal.write({'value': 2})
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE checkpoints (scope TEXT PRIMARY KEY REFERENCES runs(scope) ON DELETE CASCADE, '
                           'seq INTEGER NOT NULL, state TEXT NOT NULL)')
        connection.execute('INSERT INTO checkpoints VALUES (?, ?, ?)', ('[]', 2, '{"seq": 2}'))
        connection.execute('PRAGMA user_version = 2')
    journal = SQLiteJournal(path, durable=True)
    assert journal.positions() == (2, [2])
    assert journal.recovery(at=1) == (None, [{'value': 1}])
    assert journal.recovery(at=2) == ({'seq': 2}, [])
    with sqlite3.connect(path) as connection:
        assert connection.execute('PRAGMA user_version').fetchone() == (2,)
    for position in range(3, 21):
        journal.write({'value': position})
        journal.checkpoint({'seq': position})
        if position == 3:
            assert journal.positions() == (3, [2, 3])
    assert journal.positions() == (20, list(range(5, 21)))
    assert journal.recovery(at=4) == (None, [{'value': n} for n in range(1, 5)])
    assert journal.recovery(at=5) == ({'seq': 5}, [])
    journal.reset()
    assert journal.positions() == (0, [])
