"""
file_database.py  (v4 — hybrid design)

Architecture
------------
Core fields  → real SQLite columns  (fast native queries, indexes, ORDER BY, ranges)
Extra fields → JSON blob column      (flexible, no schema change needed)
field_index  → only for blob fields  (B-tree lookup for non-core fields)

Table layout
------------
    records
    ┌─────────────┬──────────┬─────────────────────────────┐
    │ id          │ TEXT PK  │ UUID, always present        │
    │ created_at  │ TEXT     │ ISO timestamp, auto-set     │
    │ <col> ...   │ varies   │ declared schema columns     │
    │ data        │ TEXT     │ JSON blob for extra fields  │
    └─────────────┴──────────┴─────────────────────────────┘

    field_index
    ┌───────┬───────┬─────┐
    │ field │ value │ id  │   — only for fields NOT in core columns
    └───────┴───────┴─────┘

Migration
---------
On first open of an existing v3 database (blob-only), the _migrate()
method detects the old layout and upgrades it automatically:
  1. Reads all existing records from the old `data` blob
  2. Re-creates the table with the new hybrid layout
  3. Re-inserts every record into the new structure
  4. Stores the schema version in a _meta table

Query routing
-------------
  filter field in core columns → native SQL  (WHERE col = ?)
  filter field in blob         → field_index  (JOIN field_index)
  mixed filters                → SQL for core, Python for blob remainder

Public API (unchanged from v3)
------------------------------
  db.insert(record)
  db.read(record_id, filters, order_by, limit, offset)
  db.update(record_id, changes)
  db.delete(record_id)
  db.count()
  db.clear()
  db.index_stats()
  db.create_index(field)
  db.rebuild_index()
  db.schema_info()
  db.close()
"""

import json
import uuid
import sqlite3
import os
from datetime import datetime, timezone
from typing import Any


CURRENT_VERSION = 4          # bump when migration logic changes
ALWAYS_CORE    = {"id", "created_at", "data"}   # reserved, never in schema


# ── type map: Python type → SQLite affinity ───────────────────────────
_PY_TO_SQL: dict[type, str] = {
    str:   "TEXT",
    int:   "INTEGER",
    float: "REAL",
    bool:  "INTEGER",   # SQLite has no BOOLEAN; 0/1
}


class FileDatabase:

    def __init__(
        self,
        filepath:       str        = "db.sqlite",
        schema:         dict | None = None,
        indexed_fields: list | None = None,
    ):
        """
        filepath       : path to the SQLite file (created on first use).
        schema         : dict mapping field names to Python types that
                         become real SQLite columns, e.g.
                         {"name": str, "role": str, "level": int}
                         Fields not in schema go into the JSON blob.
        indexed_fields : list of blob fields to index via field_index.
                         Core schema columns are always indexed by SQLite.
                         Pass None to index all blob fields (v3 behaviour).
        """
        if schema and (ALWAYS_CORE & set(schema)):
            raise ValueError(
                f"Schema must not include reserved fields: {ALWAYS_CORE}"
            )

        self.filepath       = filepath
        self.schema         = schema or {}
        self.indexed_fields = set(indexed_fields) if indexed_fields is not None else None
        self._core_cols     = set(self.schema.keys())   # user-declared columns

        self._conn = sqlite3.connect(filepath, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

        self._setup()
        self._migrate()

    # ------------------------------------------------------------------ #
    # Setup & migration
    # ------------------------------------------------------------------ #

    def _col_defs(self) -> str:
        """Build the column definition fragment for CREATE TABLE."""
        parts = []
        for field, py_type in self.schema.items():
            sql_type = _PY_TO_SQL.get(py_type, "TEXT")
            parts.append(f"{field} {sql_type}")
        return (",\n                ".join(parts) + "," if parts else "")

    def _setup(self) -> None:
        """Create tables if they do not exist."""
        # Check whether records table already exists (migration case)
        cur = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
        )
        records_exists = cur.fetchone() is not None

        # Always ensure meta and index tables exist
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS _meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS field_index (
                field TEXT NOT NULL,
                value TEXT NOT NULL,
                id    TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_field_value
                ON field_index (field, value);
        """)

        if not records_exists:
            # Brand new database — create with full hybrid layout
            col_defs = self._col_defs()
            self._conn.execute(f"""
                CREATE TABLE records (
                    id         TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    {col_defs}
                    data       TEXT NOT NULL DEFAULT '{{}}'
                )
            """)
            for field in self._core_cols:
                self._conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_col_{field} ON records ({field})"
                )

        self._conn.commit()

    def _migrate(self) -> None:
        """
        Detect old schema versions and upgrade automatically.

        v3 → v4 : the records table had only (id, data) columns.
                   We detect this by checking whether created_at exists.
                   If not, we rebuild the table with the new layout.
        """
        cur = self._conn.cursor()

        # Read stored version (None means brand-new or pre-versioned db)
        cur.execute("SELECT value FROM _meta WHERE key='version'")
        row = cur.fetchone()
        stored_version = int(row["value"]) if row else None

        if stored_version == CURRENT_VERSION:
            return   # already up to date

        # Detect v3 layout: no created_at column
        cur.execute("PRAGMA table_info(records)")
        existing_cols = {r["name"] for r in cur.fetchall()}

        if "created_at" not in existing_cols and "data" in existing_cols:
            print("[MIGRATE] Detected v3 database — upgrading to v4 hybrid layout...")
            self._migrate_v3_to_v4()
        elif not existing_cols:
            pass   # brand new database, nothing to migrate
        else:
            # Existing v4 db — add any new schema columns that are missing
            for field in self._core_cols:
                if field not in existing_cols:
                    sql_type = _PY_TO_SQL.get(self.schema[field], "TEXT")
                    self._conn.execute(
                        f"ALTER TABLE records ADD COLUMN {field} {sql_type}"
                    )
                    self._conn.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_col_{field} ON records ({field})"
                    )
                    print(f"[MIGRATE] Added new column '{field}' ({sql_type}).")
            self._conn.commit()

        # Stamp version
        self._conn.execute(
            "INSERT OR REPLACE INTO _meta VALUES ('version', ?)",
            (str(CURRENT_VERSION),),
        )
        self._conn.commit()
        print(f"[MIGRATE] Database is now at version {CURRENT_VERSION}.")

    def _migrate_v3_to_v4(self) -> None:
        """Upgrade a v3 blob-only database to the v4 hybrid layout."""
        cur = self._conn.cursor()

        # 1. Read all existing records from old table
        cur.execute("SELECT id, data FROM records")
        old_rows = [(r["id"], json.loads(r["data"])) for r in cur.fetchall()]

        # 2. Drop old tables
        self._conn.executescript("""
            DROP TABLE IF EXISTS field_index;
            DROP TABLE IF EXISTS records;
        """)

        # 3. Re-create with new layout
        self._setup()

        # 4. Re-insert every record using the new insert path
        for old_id, record in old_rows:
            record["id"] = old_id          # preserve original IDs
            if "created_at" not in record:
                record["created_at"] = datetime.now(timezone.utc).isoformat()
            self._insert_row(self._conn.cursor(), record)

        self._conn.commit()
        print(f"[MIGRATE] Migrated {len(old_rows)} records to hybrid layout.")

    # ------------------------------------------------------------------ #
    # Schema validation
    # ------------------------------------------------------------------ #

    def _validate(self, record: dict) -> None:
        if not self.schema:
            return
        for field, expected_type in self.schema.items():
            if field in record and not isinstance(record[field], expected_type):
                raise TypeError(
                    f"Field '{field}' must be {expected_type.__name__}, "
                    f"got {type(record[field]).__name__}: {record[field]!r}"
                )

    # ------------------------------------------------------------------ #
    # Split a record into core columns vs blob extras
    # ------------------------------------------------------------------ #

    def _split(self, record: dict) -> tuple[dict, dict]:
        """
        Separate a record into:
          core  — fields that map to real SQLite columns
          extra — everything else (goes into the JSON blob)
        Returns (core_dict, extra_dict).
        """
        core  = {"id": record["id"], "created_at": record.get(
            "created_at", datetime.now(timezone.utc).isoformat()
        )}
        extra = {}
        for k, v in record.items():
            if k in ("id", "created_at"):
                continue
            if k in self._core_cols:
                core[k] = v
            else:
                extra[k] = v
        return core, extra

    # ------------------------------------------------------------------ #
    # Row insertion (shared by insert and migrate)
    # ------------------------------------------------------------------ #

    def _insert_row(self, cur: sqlite3.Cursor, record: dict) -> None:
        core, extra = self._split(record)

        cols   = list(core.keys()) + ["data"]
        vals   = list(core.values()) + [json.dumps(extra)]
        marks  = ",".join("?" * len(cols))
        cur.execute(
            f"INSERT INTO records ({','.join(cols)}) VALUES ({marks})",
            vals,
        )
        self._index_add(cur, record["id"], extra)

    # ------------------------------------------------------------------ #
    # Index helpers (blob fields only)
    # ------------------------------------------------------------------ #

    def _should_index(self, field: str) -> bool:
        """Return True if this blob field should go into field_index."""
        if self.indexed_fields is None:
            return True                      # index everything (default)
        return field in self.indexed_fields

    def _index_add(self, cur: sqlite3.Cursor, record_id: str, extra: dict) -> None:
        rows = [
            (field, str(value), record_id)
            for field, value in extra.items()
            if self._should_index(field)
        ]
        if rows:
            cur.executemany(
                "INSERT INTO field_index (field, value, id) VALUES (?, ?, ?)",
                rows,
            )

    def _index_remove(self, cur: sqlite3.Cursor, record_id: str) -> None:
        cur.execute("DELETE FROM field_index WHERE id = ?", (record_id,))

    # ------------------------------------------------------------------ #
    # Record → dict reconstruction
    # ------------------------------------------------------------------ #

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """Merge core columns and blob extras back into one flat dict."""
        result = dict(row)
        blob   = result.pop("data", "{}")
        extra  = json.loads(blob) if blob else {}
        result.update(extra)
        return result

    # ------------------------------------------------------------------ #
    # INSERT
    # ------------------------------------------------------------------ #

    def insert(self, record: dict) -> dict:
        """
        Add a new record.
        - Core schema fields → real SQLite columns.
        - Extra fields       → JSON blob + field_index.
        - created_at is set automatically.
        Returns the inserted record.
        """
        self._validate(record)
        record = {
            **record,
            "id":         str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        with self._conn:
            cur = self._conn.cursor()
            self._insert_row(cur, record)

        print(f"[INSERT] id={record['id']}")
        return record

    # ------------------------------------------------------------------ #
    # READ
    # ------------------------------------------------------------------ #

    def read(
        self,
        record_id:  str | None        = None,
        filters:    dict[str, Any] | None = None,
        order_by:   str | None        = None,
        descending: bool              = False,
        limit:      int | None        = None,
        offset:     int               = 0,
    ) -> "list[dict] | dict | None":
        """
        Fetch records.

        record_id  → one record or None.
        filters    → dict of field=value pairs.
                     Core columns use native SQL WHERE.
                     Blob fields use field_index lookup.
        order_by   → field name to sort by (core columns only).
        descending → sort direction (default False = ASC).
        limit      → max records to return.
        offset     → skip this many records (for pagination).
        """
        cur = self._conn.cursor()

        # ── single record ────────────────────────────────────────────
        if record_id is not None:
            cur.execute("SELECT * FROM records WHERE id = ?", (record_id,))
            row = cur.fetchone()
            return self._row_to_dict(row) if row else None

        # ── build query ──────────────────────────────────────────────
        sql    = "SELECT * FROM records"
        params: list = []
        where_parts: list[str] = []

        if filters:
            core_filters = {k: v for k, v in filters.items() if k in self._core_cols | {"id", "created_at"}}
            blob_filters = {k: v for k, v in filters.items() if k not in core_filters}

            # Native SQL conditions for core columns
            for field, value in core_filters.items():
                where_parts.append(f"{field} = ?")
                params.append(value)

            # field_index subquery for blob fields
            if blob_filters:
                first_field, first_value = next(iter(blob_filters.items()))
                where_parts.append(
                    "id IN (SELECT id FROM field_index WHERE field=? AND value=?)"
                )
                params += [first_field, str(first_value)]

        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)

        # ORDER BY (core columns only — blob fields can't be sorted natively)
        if order_by:
            if order_by not in self._core_cols | {"id", "created_at"}:
                print(f"[WARN] ORDER BY '{order_by}' is a blob field — sorting skipped.")
            else:
                direction = "DESC" if descending else "ASC"
                sql += f" ORDER BY {order_by} {direction}"

        # LIMIT / OFFSET
        if limit is not None:
            sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"

        cur.execute(sql, params)
        rows = cur.fetchall()
        results = [self._row_to_dict(r) for r in rows]

        # Secondary filter for remaining blob conditions
        if filters:
            blob_filters = {k: v for k, v in filters.items() if k not in self._core_cols | {"id", "created_at"}}
            if len(blob_filters) > 1:
                # first blob field already handled via subquery; filter rest in Python
                remaining = dict(list(blob_filters.items())[1:])
                results = [r for r in results if all(r.get(k) == v for k, v in remaining.items())]

        return results

    # ------------------------------------------------------------------ #
    # UPDATE
    # ------------------------------------------------------------------ #

    def update(self, record_id: str, changes: dict) -> "dict | None":
        """
        Update fields on a record.
        Core fields → UPDATE column directly.
        Blob fields → merge into JSON blob + refresh field_index.
        """
        changes.pop("id", None)
        changes.pop("created_at", None)

        with self._conn:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM records WHERE id = ?", (record_id,))
            row = cur.fetchone()
            if not row:
                print(f"[UPDATE] id={record_id} not found.")
                return None

            record = self._row_to_dict(row)
            record.update(changes)
            self._validate(record)

            # Split updated record
            core, extra = self._split(record)

            # Build SET clause for core columns that changed
            set_parts  = ["data = ?"]
            set_params = [json.dumps(extra)]
            for field in self._core_cols:
                if field in changes:
                    set_parts.append(f"{field} = ?")
                    set_params.append(core.get(field))

            set_params.append(record_id)
            cur.execute(
                f"UPDATE records SET {', '.join(set_parts)} WHERE id = ?",
                set_params,
            )

            # Refresh blob index
            self._index_remove(cur, record_id)
            self._index_add(cur, record_id, extra)

        print(f"[UPDATE] id={record_id}")
        return record

    # ------------------------------------------------------------------ #
    # DELETE
    # ------------------------------------------------------------------ #

    def delete(self, record_id: str) -> bool:
        """Delete a record. ON DELETE CASCADE cleans field_index."""
        with self._conn:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM records WHERE id = ?", (record_id,))
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
        return self._conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def clear(self) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM records")
            self._conn.execute("DELETE FROM field_index")
        print("[CLEAR] All records deleted.")

    def rebuild_index(self) -> None:
        """Rebuild field_index for blob fields from scratch."""
        with self._conn:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM field_index")
            cur.execute("SELECT id, data FROM records")
            rows = cur.fetchall()
            for row in rows:
                extra = json.loads(row["data"] or "{}")
                self._index_add(cur, row["id"], extra)
        print(f"[REBUILD] Rebuilt index from {len(rows)} records.")

    def create_index(self, field: str) -> None:
        """Report index status for a field."""
        if field in self._core_cols:
            print(f"[INDEX] '{field}' is a core column — indexed natively by SQLite.")
            return
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM field_index WHERE field=?", (field,))
        count = cur.fetchone()[0]
        print(f"[INDEX] '{field}' (blob field): {count} index entries.")

    def index_stats(self) -> None:
        """Print a summary of core column indexes and blob field_index."""
        print("[INDEX] Core columns (native SQLite indexes):")
        if self._core_cols:
            for col in sorted(self._core_cols):
                sql_type = _PY_TO_SQL.get(self.schema[col], "TEXT")
                print(f"  {col:<20} {sql_type}")
        else:
            print("  (no schema columns declared)")

        print("\n[INDEX] Blob field index (field_index table):")
        cur = self._conn.cursor()
        cur.execute("SELECT DISTINCT field FROM field_index ORDER BY field")
        fields = [r["field"] for r in cur.fetchall()]
        if not fields:
            print("  (empty)")
        for field in fields:
            cur.execute(
                "SELECT value, COUNT(*) cnt FROM field_index WHERE field=? GROUP BY value",
                (field,),
            )
            rows = cur.fetchall()
            total = sum(r["cnt"] for r in rows)
            print(f"  '{field}': {len(rows)} unique values, {total} entries")
            for r in rows:
                print(f"    {r['value']!r:20s} -> {r['cnt']} record(s)")

    def schema_info(self) -> None:
        """Print the current schema and column layout."""
        cur = self._conn.cursor()
        cur.execute("PRAGMA table_info(records)")
        cols = cur.fetchall()
        print("[SCHEMA] Table columns:")
        for col in cols:
            tag = "(core)" if col["name"] in self._core_cols else "(fixed)"
            if col["name"] == "data":
                tag = "(blob)"
            print(f"  {col['name']:<20} {col['type']:<10} {tag}")
        if self.indexed_fields is not None:
            print(f"\n  Blob fields indexed: {sorted(self.indexed_fields)}")
        else:
            print("\n  All blob fields are indexed (default).")

    def close(self) -> None:
        self._conn.close()


# ====================================================================== #
# Demo
# ====================================================================== #

if __name__ == "__main__":
    import os

    DB = "demo_db.sqlite"
    if os.path.exists(DB):
        os.remove(DB)

    # Declare core columns — extra fields spill into blob automatically
    db = FileDatabase(DB, schema={
        "name":  str,
        "role":  str,
        "level": int,
    })

    print("\n--- SCHEMA ---")
    db.schema_info()

    print("\n--- INSERT (core fields) ---")
    alice = db.insert({"name": "Alice", "role": "engineer", "level": 3})
    bob   = db.insert({"name": "Bob",   "role": "designer", "level": 2})
    carol = db.insert({"name": "Carol", "role": "engineer", "level": 5})

    print("\n--- INSERT (with extra blob fields) ---")
    dave = db.insert({
        "name": "Dave", "role": "manager", "level": 4,
        "country": "Kenya",      # blob field
        "hobby":   "cycling",    # blob field
    })

    print("\n--- SELECT * ---")
    for r in db.read():
        print(" ", r)

    print("\n--- SELECT WHERE role=engineer (native SQL) ---")
    for r in db.read(filters={"role": "engineer"}):
        print(" ", r)

    print("\n--- SELECT WHERE country=Kenya (blob index) ---")
    for r in db.read(filters={"country": "Kenya"}):
        print(" ", r)

    print("\n--- ORDER BY level DESC ---")
    for r in db.read(order_by="level", descending=True):
        print(f"  {r['name']:<10} level={r['level']}")

    print("\n--- PAGINATION: limit=2 offset=1 ---")
    for r in db.read(order_by="level", limit=2, offset=1):
        print(f"  {r['name']:<10} level={r['level']}")

    print("\n--- UPDATE Bob (core + blob) ---")
    db.update(bob["id"], {"level": 4, "country": "Uganda"})
    print(db.read(record_id=bob["id"]))

    print("\n--- DELETE Carol ---")
    db.delete(carol["id"])

    print("\n--- INDEX STATS ---")
    db.index_stats()

    print(f"\nRecords remaining: {db.count()}")
    db.close()