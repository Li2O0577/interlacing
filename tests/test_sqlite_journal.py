"""SQLite journal transactions, snapshots, ordering and scope lifecycle."""
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from interlacing import SQLiteJournal
from interlacing.engine import JournalError


def test_readers_see_a_committed_snapshot_and_do_not_create_a_database(tmp_path):
    path = tmp_path / 'nested #1' / 'journal.sqlite3'
    journal = SQLiteJournal(path)
    assert journal.read() == [] and not path.exists()

    journal.write({'value': 1})
    journal.write({'value': 2})
    records = journal.inspect()
    assert next(records) == {'value': 1}
    SQLiteJournal(path).write({'value': 3})
    assert list(records) == [{'value': 2}]
    assert journal.read() == [{'value': 1}, {'value': 2}, {'value': 3}]


def test_concurrent_writers_assign_unique_ordered_positions(tmp_path):
    path = tmp_path / 'journal.sqlite3'
    journal = SQLiteJournal(path)
    journal.write({'value': 0})

    def append(value):
        SQLiteJournal(path).write({'value': value})

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(append, range(1, 25)))
    assert sorted(record['value'] for record in journal.read()) == list(range(25))
    with closing(sqlite3.connect(path)) as connection:
        assert [seq for seq, in connection.execute("SELECT seq FROM run_events ORDER BY seq")] == list(range(1, 26))


def test_reset_keeps_child_history_and_discard_removes_only_its_subtree(tmp_path):
    journal = SQLiteJournal(tmp_path / 'journal.sqlite3')
    child = journal.inner('box', 1)
    grandchild = child.inner('inner', 1)
    sibling = journal.inner('box', 2)
    for journal_part, value in ((journal, 'root'), (child, 'child'), (grandchild, 'grandchild'), (sibling, 'sibling')):
        journal_part.write({'value': value})

    journal.reset()
    assert journal.read() == []
    assert grandchild.read() == [{'value': 'grandchild'}]
    child.discard()
    assert child.read() == grandchild.read() == []
    assert sibling.read() == [{'value': 'sibling'}]
    journal.write({'value': 'new root'})
    with closing(sqlite3.connect(journal.path)) as connection:
        assert connection.execute("SELECT seq FROM run_events WHERE scope = '[]'").fetchall() == [(1,)]


def test_a_failed_write_rolls_back_new_scopes_and_events(tmp_path):
    journal = SQLiteJournal(tmp_path / 'journal.sqlite3')
    journal.write({'kind': 'run'})
    with closing(sqlite3.connect(journal.path)) as connection:
        connection.execute("CREATE TRIGGER refuse_child BEFORE INSERT ON run_events "
                           "WHEN NEW.scope <> '[]' BEGIN SELECT RAISE(ABORT, 'refused child'); END")
    child = journal.inner('outer', 1).inner('inner', 1)
    with pytest.raises(JournalError, match='refused child'):
        child.write({'kind': 'run'})
    assert journal.read() == [{'kind': 'run'}] and child.read() == []
    with closing(sqlite3.connect(journal.path)) as connection:
        assert connection.execute("SELECT scope FROM runs").fetchall() == [('[]',)]
