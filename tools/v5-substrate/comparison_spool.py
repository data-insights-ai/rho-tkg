"""Disposable raw verifier state; no graph model, durable result or resume API.

Callers validate original records/effects before insertion or replacement, keep
their raw artifacts separately, and supply every quota. SQLite's page-cache
setting is a finite target, not a process RSS limit. Only one statement/row is
live at a time; the indexed walk needs no sorting or temporary SQL tables.
"""
from dataclasses import asdict, dataclass
from pathlib import Path
import shutil
import sqlite3
import struct
import tempfile

PAGE_BYTES = 4096
WALK = "SELECT key,value FROM state WHERE namespace=? AND key>? ORDER BY key LIMIT 1"


class SpoolError(ValueError):
    """Refused state, quota/storage failure, or invalid private spool use."""


def u64(value):
    """Preserve unsigned coordinates and their order without SQLite integers."""
    if type(value) is not int or not 0 <= value < 1 << 64:
        raise SpoolError("unsigned coordinate type/range")
    return struct.pack(">Q", value)


@dataclass(frozen=True)
class Policy:
    max_disk_bytes: int
    cache_bytes: int
    max_key_bytes: int
    max_value_bytes: int

    def validate(self):
        if any(type(v) is not int or v < 1 for v in asdict(self).values()):
            raise SpoolError("positive integer spool quotas required")
        if (self.max_disk_bytes % PAGE_BYTES or self.max_disk_bytes < 2 * PAGE_BYTES
                or self.max_disk_bytes // PAGE_BYTES >= 1 << 31
                or self.cache_bytes % PAGE_BYTES or self.cache_bytes > self.max_disk_bytes
                or self.max_key_bytes + self.max_value_bytes > self.max_disk_bytes):
            raise SpoolError("invalid page/cache/record quota")


class Spool:
    def __init__(self, directory, policy):
        policy.validate()
        self._policy, self._db, self._walking = policy, None, False
        self.directory, self.path = None, None
        self._peak_file_bytes, self._peak_allocated_bytes = 0, 0
        try:
            owner = Path(directory).resolve(strict=True)
            if not owner.is_dir():
                raise SpoolError("caller-owned directory required")
            self.directory = Path(tempfile.mkdtemp(prefix=".cost-spool-", dir=owner))
            self.path = self.directory / "state.sqlite"
            self.path.touch(mode=0o600, exist_ok=False)
            self._db = sqlite3.connect(self.path, isolation_level=None, cached_statements=8)
            # This scratch database is discarded on EVERY failure, including
            # duplicate or partial SQLite work. It publishes no durable result.
            settings = {"page_size": PAGE_BYTES, "max_page_count": policy.max_disk_bytes // PAGE_BYTES,
                        "cache_size": policy.cache_bytes // PAGE_BYTES, "mmap_size": 0,
                        "journal_mode": "off", "synchronous": 0, "temp_store": 2,
                        "automatic_index": 0}
            for name, value in settings.items():
                self._query(f"PRAGMA {name}={value}")
                if self._query(f"PRAGMA {name}")[0][0] != value:
                    raise SpoolError("SQLite refused explicit policy: " + name)
            self._db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,
                              max(PAGE_BYTES, policy.max_key_bytes + policy.max_value_bytes + 256))
            self._query("CREATE TABLE state(namespace BLOB,key BLOB,value BLOB NOT NULL,"
                        "PRIMARY KEY(namespace,key)) WITHOUT ROWID")
            plan, _ = self._query("EXPLAIN QUERY PLAN " + WALK, (b"n", b""))
            self._walk_plan = plan[-1]
            if "PRIMARY KEY" not in self._walk_plan or "TEMP" in self._walk_plan:
                raise SpoolError("walk requires primary-key order without temp sorting")
            self._measure()
        except (OSError, sqlite3.Error, SpoolError) as error:
            self._abort(str(error), error)

    def _abort(self, message, cause=None):
        try:
            self.close()
        except (OSError, sqlite3.Error) as cleanup:
            raise SpoolError(message + "; cleanup failed: " + str(cleanup)) from cause
        raise SpoolError(message) from cause

    def _query(self, sql, parameters=()):
        if self._db is None:
            raise SpoolError("comparison spool closed")
        try:
            cursor = self._db.execute(sql, parameters)
            try:
                return cursor.fetchone(), cursor.rowcount
            finally:
                cursor.close()
        except sqlite3.IntegrityError as error:
            self._abort("duplicate state/original/transaction identity", error)
        except sqlite3.Error as error:
            self._abort("spool capacity/storage/query refused: " + str(error), error)

    def _key(self, namespace, key):
        if type(namespace) is not bytes or type(key) is not bytes or not namespace or not key:
            self._abort("nonempty raw bytes namespace/key required")
        if len(namespace) + len(key) > self._policy.max_key_bytes:
            self._abort("spool key quota exceeded")

    def _value(self, value):
        if type(value) is not bytes:
            self._abort("raw bytes value required")
        if len(value) > self._policy.max_value_bytes:
            self._abort("spool value quota exceeded")

    def _measure(self):
        try:
            if list(self.directory.iterdir()) != [self.path]:
                self._abort("unexpected SQLite temporary/journal file")
            info = self.path.stat()
            allocated = info.st_blocks * 512
            self._peak_file_bytes = max(self._peak_file_bytes, info.st_size)
            self._peak_allocated_bytes = max(self._peak_allocated_bytes, allocated)
            if max(info.st_size, allocated) > self._policy.max_disk_bytes:
                self._abort("spool disk quota exceeded")
        except OSError as error:
            self._abort("spool measurement failed", error)

    def find(self, namespace, key):
        self._key(namespace, key)
        row, _ = self._query("SELECT value FROM state WHERE namespace=? AND key=?", (namespace, key))
        return None if row is None else row[0]

    def lookup(self, namespace, key):
        value = self.find(namespace, key)
        if value is None:
            self._abort("missing required spool state")
        return value

    def insert(self, namespace, key, value):
        self._key(namespace, key)
        self._value(value)
        if self._walking:
            self._abort("mutation during keyed walk")
        self._query("INSERT INTO state VALUES(?,?,?)", (namespace, key, value))
        self._measure()

    def replace(self, namespace, key, expected, value):
        self._key(namespace, key)
        self._value(expected)
        self._value(value)
        if self._walking:
            self._abort("mutation during keyed walk")
        if self.lookup(namespace, key) != expected:
            self._abort("exact predecessor mismatch")
        _, changed = self._query("UPDATE state SET value=? WHERE namespace=? AND key=? AND value=?",
                                 (value, namespace, key, expected))
        if changed != 1:
            self._abort("exact predecessor replacement failed")
        self._measure()

    def walk(self, namespace):
        self._key(namespace, b"x")
        if self._walking:
            self._abort("overlapping keyed walk")
        self._walking = True
        try:
            after = b""
            while True:
                row, _ = self._query(WALK, (namespace, after))
                if row is None:
                    break
                after = row[0]
                yield row
        finally:
            self._walking = False

    def report(self):
        return {"policy": asdict(self._policy), "page_bytes": PAGE_BYTES,
                "sqlite_version": sqlite3.sqlite_version, "walk_plan": self._walk_plan,
                "journal_mode": "off", "mmap_bytes": 0, "temp_store": "memory; no SQL temp tables/sorts",
                "cached_statements": 8, "cache_is_process_rss_limit": False,
                "peak_file_bytes": self._peak_file_bytes, "peak_allocated_bytes": self._peak_allocated_bytes}

    def close(self):
        database, self._db = self._db, None
        try:
            if database is not None:
                database.close()
        finally:
            if self.directory is not None and self.directory.exists():
                shutil.rmtree(self.directory)

    def __enter__(self):
        if self._db is None:
            raise SpoolError("comparison spool closed")
        return self

    def __exit__(self, kind, error, traceback):
        self.close()
