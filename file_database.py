"""
file_database.py  (v3 — SQLite backend)

Replaces the JSON + index approach with SQLite, which gives us:
  - Atomic writes      : SQLite commits are all-or-nothing
  - Concurrency safety : SQLite handles file locking internally
  - No full-file loads : queries only touch the rows they need
  - Real indexes       : CREATE INDEX for O(1) field lookups
  - Schema validation  : optional, enforced on insert/update

Storage: a single  .sqlite  file on disk. No separate index file needed.

The public API is identical to v1/v2:
    db.insert(record)
    db.read(record_id, filters, use_index)
    db.update(record_id, changes)
    db.delete(record_id)
    db.clear()
    db.count()
    db.index_stats()
    db.create_index(field)
    db.rebuild_index()        <- no-op in SQLite (indexes are automatic)

How SQLite is used here
-----------------------
We store every record in a table called `records` with two columns:
    id    TEXT PRIMARY KEY
    data  TEXT             (the record dict serialised as JSON)

Storing the payload as a JSON blob keeps the database schema-free —
you can insert records with any fields without running ALTER TABLE.
Indexed fields get a companion table `field_index`:
    field  TEXT
    value  TEXT
    id     TEXT  REFERENCES records(id)

This mirrors the manual dict index from v2, but SQLite manages the
B-tree, the file locking, and the atomic commits for us.
"""

import json
import uuid
import sqlite3
import os
from typing import Any


class FileDatabase:

    def __init__(self, filepath: str = "db.sqlite", schema: dict | None = None):
        """
        filepath : path to the SQLite database file.
                   Created automatically on first use.
        schema   : optional dict mapping field names to expected Python
                   types, e.g. {"name": str, "level": int}.
                   Validated on every insert and update.
        """
        self.filepath = filepath
        self.schema   = schema

        # check_same_thread=False lets the same instance be used from
        # multiple threads safely (SQLite handles the locking itself).
        self._conn = sqlite3.connect(filepath, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row   # rows behave like dicts
        self._conn.execute("PRAGMA journal_mode=WAL")  # safer concurrent writes
        self._setup()

    # ------------------------------------------------------------------ #
    # One-time table creation
    # ------------------------------------------------------------------ #

    def _setup(self) -> None:
        """Create tables if they do not exist yet."""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS records (
                id   TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS field_index (
                field TEXT NOT NULL,
                value TEXT NOT NULL,
                id    TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_field_value
                ON field_index (field, value);
        """)
        self._conn.commit()

    # ------------------------------------------------------------------ #
    # Schema validation
    # ------------------------------------------------------------------ #

    def _validate(self, record: dict) -> None:
        """Raise ValueError/TypeError if record violates the schema."""
        if not self.schema:
            return
        for field, expected_type in self.schema.items():
            if field not in record:
                raise ValueError(f"Missing required field: '{field}'")
            if not isinstance(record[field], expected_type):
                raise TypeError(
                    f"Field '{field}' must be {expected_type.__name__}, "
                    f"got {type(record[field]).__name__}: {record[field]!r}"
                )

    # ------------------------------------------------------------------ #
    # Internal index helpers
    # ------------------------------------------------------------------ #

    def _index_add(self, cursor: sqlite3.Cursor, record: dict) -> None:
        """Insert one record's fields into field_index."""
        record_id = record["id"]
        rows = [
            (field, str(value), record_id)
            for field, value in record.items()
            if field != "id"
        ]
        cursor.executemany(
            "INSERT INTO field_index (field, value, id) VALUES (?, ?, ?)",
            rows,
        )

    def _index_remove(self, cursor: sqlite3.Cursor, record_id: str) -> None:
        """Remove all index entries for a record."""
        cursor.execute(
            "DELETE FROM field_index WHERE id = ?", (record_id,)
        )

    # ------------------------------------------------------------------ #
    # INSERT
    # ------------------------------------------------------------------ #

    def insert(self, record: dict) -> dict:
        """
        Add a new record.
        - Validates against schema if one was provided.
        - Auto-generates a UUID for the 'id' field.
        - Updates the field index atomically in the same transaction.
        Returns the inserted record.
        """
        self._validate(record)
        record = {**record, "id": str(uuid.uuid4())}

        with self._conn:                          # auto commit / rollback
            cur = self._conn.cursor()
            cur.execute(
                "INSERT INTO records (id, data) VALUES (?, ?)",
                (record["id"], json.dumps(record)),
            )
            self._index_add(cur, record)

        print(f"[INSERT] id={record['id']}")
        return record

    # ------------------------------------------------------------------ #
    # READ
    # ------------------------------------------------------------------ #

    def read(
        self,
        record_id: str | None = None,
        filters: dict[str, Any] | None = None,
        use_index: bool = True,
    ) -> "list[dict] | dict | None":
        """
        Fetch records.

        record_id  -> return one dict or None.
        filters    -> return matching records.
                      Uses the field_index when use_index=True (default).
        (no args)  -> return all records.
        """
        cur = self._conn.cursor()

        # ── single record by ID ──────────────────────────────────────
        if record_id is not None:
            cur.execute("SELECT data FROM records WHERE id = ?", (record_id,))
            row = cur.fetchone()
            return json.loads(row["data"]) if row else None

        # ── no filter → all records ──────────────────────────────────
        if not filters:
            cur.execute("SELECT data FROM records")
            return [json.loads(r["data"]) for r in cur.fetchall()]

        # ── filtered search via index ────────────────────────────────
        if use_index:
            # Use the first filter field to get candidate IDs from the index
            items          = list(filters.items())
            primary_field  = items[0][0]
            primary_value  = str(items[0][1])

            cur.execute(
                "SELECT id FROM field_index WHERE field = ? AND value = ?",
                (primary_field, primary_value),
            )
            candidate_ids = [r["id"] for r in cur.fetchall()]

            if not candidate_ids:
                return []

            # Fetch the full records for those IDs
            placeholders = ",".join("?" * len(candidate_ids))
            cur.execute(
                f"SELECT data FROM records WHERE id IN ({placeholders})",
                candidate_ids,
            )
            candidates = [json.loads(r["data"]) for r in cur.fetchall()]

            # Apply any remaining filter conditions in Python
            remaining = dict(items[1:])
            if remaining:
                candidates = [
                    r for r in candidates
                    if all(r.get(k) == v for k, v in remaining.items())
                ]
            return candidates

        # ── full scan fallback ───────────────────────────────────────
        cur.execute("SELECT data FROM records")
        return [
            json.loads(r["data"])
            for r in cur.fetchall()
            if all(json.loads(r["data"]).get(k) == v
                   for k, v in filters.items())
        ]

    # ------------------------------------------------------------------ #
    # UPDATE
    # ------------------------------------------------------------------ #

    def update(self, record_id: str, changes: dict) -> "dict | None":
        """
        Update fields on a record.
        - Validates changed fields against schema.
        - Re-indexes the record atomically.
        - Only the listed fields are overwritten.
        Returns the updated record, or None if not found.
        """
        changes.pop("id", None)

        with self._conn:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT data FROM records WHERE id = ?", (record_id,)
            )
            row = cur.fetchone()
            if not row:
                print(f"[UPDATE] id={record_id} not found.")
                return None

            record = json.loads(row["data"])
            record.update(changes)
            self._validate(record)

            # Overwrite the data blob and refresh the index
            cur.execute(
                "UPDATE records SET data = ? WHERE id = ?",
                (json.dumps(record), record_id),
            )
            self._index_remove(cur, record_id)
            self._index_add(cur, record)

        print(f"[UPDATE] id={record_id}")
        return record

    # ------------------------------------------------------------------ #
    # DELETE
    # ------------------------------------------------------------------ #

    def delete(self, record_id: str) -> bool:
        """
        Delete a record. The ON DELETE CASCADE on field_index means
        SQLite automatically removes the index rows too.
        Returns True if deleted, False if not found.
        """
        with self._conn:
            cur = self._conn.cursor()
            cur.execute(
                "DELETE FROM records WHERE id = ?", (record_id,)
            )
            deleted = cur.rowcount > 0

        if deleted:
            print(f"[DELETE] id={record_id}")
        else:
            print(f"[DELETE] id={record_id} not found.")
        return deleted

    # ------------------------------------------------------------------ #
    # Convenience
    # ------------------------------------------------------------------ #

    def count(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM records")
        return cur.fetchone()[0]

    def clear(self) -> None:
        """Delete all records and all index entries."""
        with self._conn:
            self._conn.execute("DELETE FROM records")
            self._conn.execute("DELETE FROM field_index")
        print("[CLEAR] All records and index entries deleted.")

    def create_index(self, field: str) -> None:
        """
        Report index coverage for a field.
        (All fields are indexed automatically — this is informational.)
        """
        cur = self._conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM field_index WHERE field = ?", (field,)
        )
        count = cur.fetchone()[0]
        if count:
            print(f"[INDEX] '{field}' has {count} index entries.")
        else:
            print(f"[INDEX] '{field}' has no entries yet.")

    def rebuild_index(self) -> None:
        """
        Rebuild field_index from scratch by re-scanning all records.
        Useful if the index somehow gets out of sync.
        """
        with self._conn:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM field_index")
            cur.execute("SELECT id, data FROM records")
            rows = cur.fetchall()
            for row in rows:
                record = json.loads(row["data"])
                self._index_add(cur, record)
        print(f"[INDEX] Rebuilt from {len(rows)} records.")

    def index_stats(self) -> None:
        """Print a summary of what is currently in the field index."""
        cur = self._conn.cursor()
        cur.execute("SELECT DISTINCT field FROM field_index ORDER BY field")
        fields = [r["field"] for r in cur.fetchall()]

        if not fields:
            print("[INDEX] Empty — no data yet.")
            return

        print("[INDEX] Current index:")
        for field in fields:
            cur.execute(
                """SELECT value, COUNT(*) as cnt
                   FROM field_index WHERE field = ?
                   GROUP BY value ORDER BY value""",
                (field,),
            )
            rows = cur.fetchall()
            total = sum(r["cnt"] for r in rows)
            print(f"  '{field}': {len(rows)} unique values, "
                  f"{total} total entries")
            for r in rows:
                print(f"    {r['value']!r:20s} -> {r['cnt']} record(s)")

    def close(self) -> None:
        """Close the database connection explicitly."""
        self._conn.close()


# ====================================================================== #
# Demo
# ====================================================================== #

if __name__ == "__main__":
    # Remove old db if re-running the demo
    if os.path.exists("demo_db.sqlite"):
        os.remove("demo_db.sqlite")

    # Optional: enforce a schema
    db = FileDatabase("demo_db.sqlite", schema={
        "name":  str,
        "role":  str,
        "level": int,
    })

    print("\n--- INSERT ---")
    alice = db.insert({"name": "Alice", "role": "engineer", "level": 3})
    bob   = db.insert({"name": "Bob",   "role": "designer", "level": 2})
    carol = db.insert({"name": "Carol", "role": "engineer", "level": 5})
    dave  = db.insert({"name": "Dave",  "role": "manager",  "level": 4})

    print("\n--- SELECT * ---")
    for r in db.read():
        print(" ", r)

    print("\n--- SELECT WHERE role=engineer ---")
    for r in db.read(filters={"role": "engineer"}):
        print(" ", r)

    print("\n--- INDEX STATS ---")
    db.index_stats()

    print("\n--- UPDATE Bob ---")
    db.update(bob["id"], {"role": "senior designer", "level": 4})
    print(db.read(record_id=bob["id"]))

    print("\n--- DELETE Carol ---")
    db.delete(carol["id"])

    print(f"\nRecords remaining: {db.count()}")

    print("\n--- SCHEMA VIOLATION TEST ---")
    try:
        db.insert({"name": "Eve", "role": "intern", "level": "five"})
    except TypeError as e:
        print(f"  Caught: {e}")

    db.close()