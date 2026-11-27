"""Back up a PostgreSQL snapshot and its immutable content to a new private directory."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
from tempfile import TemporaryDirectory

import psycopg


def copy_objects(db, source, target, manifest):
    target.mkdir()
    with db.cursor(name="files") as cursor:
        cursor.execute("SELECT DISTINCT digest,size FROM il_files")
        for digest, size in cursor:
            if not re.fullmatch("[0-9a-f]{64}", digest):
                raise ValueError("invalid file digest in database")
            relative = Path(digest[:2], digest[2:])
            dest = target / relative
            dest.parent.mkdir(exist_ok=True)
            shutil.copyfile(source / relative, dest)
            with dest.open("rb") as stream:
                if dest.stat().st_size != size or hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                    raise ValueError(f"content verification failed: {digest}")
            manifest.write(f"{digest}  content/{relative.as_posix()}\n")


def backup(target):
    source = Path(os.environ["INTERLACING_CONTENT_DIR"]).resolve(strict=True)
    target = target.resolve()
    if not source.is_dir() or target.is_relative_to(source) or target.exists():
        raise ValueError("choose a new backup directory outside the content store")
    with TemporaryDirectory(dir=target.parent, prefix=".interlacing-backup-") as staging:
        stage = Path(staging)
        archive = stage / "database.dump"
        with (stage / "SHA256SUMS").open("w", encoding="ascii") as manifest, psycopg.connect("") as db:
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            snapshot, = db.execute("SELECT pg_export_snapshot()").fetchone()
            subprocess.run(["pg_dump", "--format=custom", "--snapshot", snapshot,
                            "--file", str(archive)], check=True)
            with archive.open("rb") as stream:
                checksum = hashlib.file_digest(stream, "sha256").hexdigest()
            manifest.write(f"{checksum}  database.dump\n")
            copy_objects(db, source, stage / "content", manifest)
        # Reserve the destination even if another backup chose the same name.
        target.mkdir(mode=0o700)
        try:
            stage.replace(target)
        except OSError:
            target.rmdir()
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    if not os.environ.get("PGDATABASE"):
        parser.error("set PGDATABASE and the libpq connection environment for the source")
    backup(args.destination)
