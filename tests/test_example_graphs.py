"""The examples under examples/graphs run as written."""
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[1] / 'examples' / 'graphs'


@pytest.fixture(autouse=True)
def on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLES))
    yield
    for name in ('chain_with_program', 'review_loop', 'governed_tree', 'camera_to_arm', 'files_checked', 'blocks_joined', 'grown_at_run_time', 'planner_and_arm', 'desks_between_blocks', 'plain_functions', 'folder_as_node', 'changed_while_running', 'served_elsewhere', 'corrected_estimate', 'rejected_answer', 'hired_team', 'directed_rejection'):
        sys.modules.pop(name, None)


def test_chain_with_program(capsys):
    import chain_with_program
    assert chain_with_program.main().outputs == {'out': ['the x3, and x2, cat x1']}


def test_files_checked(tmp_path):
    import files_checked
    result = files_checked.main(tmp_path)
    assert result.last('out') == 'chart.svg (28 bytes) drawn from data/table.csv (25 bytes)'
    assert [(p['node'], p['handled']) for p in result.problems] == [('draw', 'rejected')] and result.rounds['draw'] == 1


def test_blocks_joined():
    import blocks_joined
    result = blocks_joined.main()
    assert result.last('out') == ' | '.join(['One design, three uses.'] * 3) and result.rounds == {'first': 1, 'panel': 1, 'join': 1}


def test_grown_at_run_time(capsys):
    import grown_at_run_time
    result, refused = grown_at_run_time.main()
    assert result.last('out')['outputs'] == {'out': [['mean 6', 'largest 9', 'spread 6']]} and result.rounds == {'plan': 1, 'grow': 1}
    assert 'exit cannot be found' in refused['error']


def test_planner_and_arm(capsys):
    import planner_and_arm
    result, bench = planner_and_arm.main()
    assert result.last('out')['block'] == planner_and_arm.GOAL and bench.moves == 3 and result.used['plans'] == 2
    assert [p['handled'] for p in result.problems] == ['rejected'] and 'must be 5 to 85 mm apart' in result.problems[0]['problem']


def test_desks_between_blocks(capsys):
    import desks_between_blocks
    assert desks_between_blocks.main().last('out') == {
        'count': 2, 'order': ['measured salt at ESTUARY', 'measured tide at HARBOUR']}


def test_review_loop():
    import review_loop
    result = review_loop.main()
    assert result.rounds == {'writer': 3, 'reviewer': 3} and result.last('out').startswith('draft 3:') and result.limited == ()


def test_governed_tree_asks_and_survives_a_restart(tmp_path):
    import governed_tree
    asked = []
    assert governed_tree.main(lambda request: (asked.append(request), {'approved': False})[1]).last('out') == 'held back'
    assert asked == [{'results': ['survey surveyed the bridge', 'build built the bridge']}]

    present = []

    def operator(request):                     # the same operator both times; the first time they are interrupted
        if not present:
            raise KeyboardInterrupt
        return {'approved': True}
    with pytest.raises(KeyboardInterrupt):
        governed_tree.main(operator, tmp_path / 'run.jsonl')
    present.append(True)
    result = governed_tree.main(operator, tmp_path / 'run.jsonl')
    assert result.last('out') == 'released' and result.rounds == {'lead': 3, 'survey': 1, 'build': 1, 'operator': 1}


def test_camera_to_arm():
    import camera_to_arm
    arm, result = camera_to_arm.main()
    assert arm.log[0] == 'connected' and arm.log[-1] == 'disconnected' and arm.position == result.outputs['at'][-1]
    assert 1 <= len(result.outputs['at']) <= 4 and all(0 < at <= 5 for at in result.outputs['at'])
    arm = camera_to_arm.Arm()
    from interlacing.engine import Engine
    from interlacing.protocols import DEFAULT
    steady = Engine(camera_to_arm.build(arm, [{'target': 5}] * 4, speed=2), DEFAULT).run()   # one thread: only the newest frame is seen
    assert steady.outputs['at'] == [2] and arm.log == ['connected', 'moved to 2', 'disconnected']


def test_plain_functions(capsys):
    import plain_functions
    counted, short = plain_functions.main()
    assert counted.last('out') == '5 words, 15 letters' and len(short.last('out')) <= 40


def test_folder_as_node(capsys):
    import folder_as_node
    result = folder_as_node.main()
    assert result.last('common') == [['the', 3], ['and', 2]] and result.last('total') == 8


def test_changed_while_running(capsys):
    import changed_while_running
    result = changed_while_running.main()
    assert result.outputs == {'part0': ['read the data: done'], 'part1': ['fit the model: done'],
                              'part2': ['write it up: done']}


def test_served_elsewhere(capsys):
    import served_elsewhere
    result = served_elsewhere.main()
    assert result.last('out') == 'measured 20.5' and result.used == {'readings': 1}


def test_corrected_estimate(capsys):
    import corrected_estimate
    result = corrected_estimate.main()
    assert result.outputs['quote'] == [868.0]          # run again with the careful area, put right in place
    assert result.outputs['note'] == ['First look: about 26.2 m².', 'Revised: the floor is 24.8 m², measured room by room.']


def test_rejected_answer(capsys):
    import rejected_answer
    result = rejected_answer.main()
    assert result.last('verdict') == 'approved: 1. measure 2. cut 3. check the fit 4. glue'


def test_hired_team(capsys):
    import hired_team
    result, engine, lead = hired_team.main()
    assert result.last('words') == '9' and [node.id for node in engine.graph.nodes] == ['lead'] and lead.hires == 2


def test_directed_rejection(capsys):
    import directed_rejection
    result = directed_rejection.main()
    assert result.last('area') == 75.0
    assert result.last('log') == 'logged: quick: 74 m2, measured after a rejection (50 is far from 76)'
    assert result.outputs['log'] == [result.last('log')] and result.rounds['merge'] == 1


@pytest.mark.parametrize('wrong_units', [True, False])
def test_simulation_review_after_interrupted_answer(tmp_path, wrong_units):
    from simulation_review import experiment
    from interlacing import Engine, Journal, RunError
    calls = []
    journal = Journal(tmp_path / 'journal.jsonl')

    def interrupt():
        raise RuntimeError('interrupted assessment')

    with pytest.raises(RunError, match='interrupted assessment'):
        Engine(experiment(wrong_units, calls, interrupt), journal=journal).run({'steps': 100})
    result = Engine(experiment(wrong_units, calls), journal=journal).resume()
    assert calls == [100] and result.last('report') == 0.366
    assert result.last('verdict') == ('corrected' if wrong_units else 'explained')
    actions = [r['action'] for r in journal.read() if r['kind'] == 'control' and r['nodes'] == ['b2']]
    assert actions == (['pause', 'resume'] if wrong_units else [])


def test_simulation_recovers_when_process_exits_after_pause(tmp_path):
    import subprocess
    from simulation_review import experiment
    from interlacing import Engine, Journal
    from interlacing.inspection import explain
    from interlacing.spec import dump

    path = tmp_path / 'events.jsonl'
    script = """
import os
import sys
from interlacing import Engine, Journal
from simulation_review import experiment

def cut(event):
    if event['event'] == 'control' and event['action'] == 'pause':
        os._exit(37)

Engine(experiment(True, []), journal=Journal(sys.argv[1]), observe=cut).run({'steps': 100})
"""
    proc = subprocess.run([sys.executable, '-c', script, str(path)], cwd=EXAMPLES, timeout=20)
    assert proc.returncode == 37
    calls = []
    graph = experiment(True, calls)
    resumed = Engine(graph, journal=Journal(path)).resume()
    assert calls == [] and resumed.last('report') == 0.366
    timeline = explain(dump(graph), path, 'done', [], timeline=True)['timeline']
    assert [e['action'] for e in timeline if e['kind'] == 'control'] == ['pause', 'resume']
    assert [e['answer'] for e in timeline if e['kind'] == 'verdict'] == ['corrected']
    assert sum(e['kind'] == 'challenged' for e in timeline) == 2
    assert sum(e.get('amounts', {}).get('simulations', 0) for e in timeline) == 1
