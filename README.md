# Toothless-Database
# FileDatabase

A lightweight, file-backed database built in Python using SQLite. Designed for personal projects, CLI tools, and learning how databases work from the ground up. Comes with a full interactive command-line interface.

---

## What's included

```
file_database.py   # The database engine
cli.py             # Interactive command-line interface
```

Your data is stored in a single `.sqlite` file — no server, no installation beyond Python itself.

---

## Quick start

```bash
# Launch the interactive CLI
python cli.py

# Or point it at a specific file
python cli.py mydata.sqlite
```

```
db> INSERT name=Alice role=engineer level=3
db> SELECT *
db> SELECT WHERE role=engineer
db> UPDATE <id> level=5
db> DELETE <id>
db> EXIT
```

See [CLI commands](#cli-commands) for the full reference.

---

## Scalability

FileDatabase is designed for **small to medium datasets** — personal tools, prototypes, and single-user applications.

| Dataset size     | Performance |
|------------------|-------------|
| Up to ~10,000 records   | Fast — sub-millisecond reads |
| 10,000 – 100,000 records | Usable — indexed queries stay quick, full scans slow |
| 100,000+ records  | Not recommended — consider PostgreSQL |

**What helps it scale:**

- **Field index** — a `field_index` table maps every field value directly to matching record IDs. A query like `SELECT WHERE role=engineer` looks up the index first and fetches only the matching rows, rather than scanning every record.
- **SQLite B-tree index** — the `field_index` table has a `CREATE INDEX` on `(field, value)`, so the lookup itself is O(log n), not O(n).
- **WAL mode** — Write-Ahead Logging (`PRAGMA journal_mode=WAL`) allows reads and writes to happen concurrently without blocking each other, which helps under moderate load.

**Where it won't scale:**

- Every record's payload is stored as a JSON blob in a single `data` column. SQLite cannot filter inside the blob natively — all field filtering goes through the `field_index` table or a Python-side scan.
- There is no connection pooling or multi-process coordination. For high-concurrency web applications, use PostgreSQL.

---

## Security

FileDatabase is a **local, single-user tool**. It has no network interface, no authentication layer, and no concept of users or permissions — by design.

**What is protected:**

- **Atomic writes** — every write operation runs inside a SQLite transaction (`with self._conn:`). If the process crashes mid-write, the database rolls back to the last clean state automatically. No half-written records, no corrupt files.
- **WAL journaling** — the WAL journal means a power cut during a write leaves the main database file intact. SQLite recovers automatically on the next open.
- **Schema validation** — if you initialise the database with a `schema=` argument, every insert and update is type-checked before touching the database. Bad data raises a clear `TypeError` before anything is written.

**What is not protected:**

- **No encryption** — the `.sqlite` file is readable by anyone with access to the filesystem. Do not store passwords, tokens, or sensitive personal data without encrypting the file first (e.g. with `SQLCipher`).
- **No access control** — anyone who can run Python and access the file can read or modify all records.
- **No SQL injection protection in the CLI** — the CLI parses `field=value` pairs and passes them as parameterised queries to SQLite, so injection via the query interface is not possible. However, the database itself performs no sanitisation of stored values.

**Recommended use:** local scripts, developer tooling, personal data that does not need to be shared or accessed over a network.

---

## Speed

Read and write performance comes from three layers working together.

**Indexed reads (fast path)**

When you query a field that exists in `field_index`, the database:

1. Looks up `(field, value)` in the B-tree index — O(log n)
2. Retrieves the matching record IDs — typically a very small set
3. Fetches only those rows from the `records` table by primary key — O(1) per row

A `SELECT WHERE role=engineer` on 50,000 records returns in under 1 ms if the result set is small.

**Unindexed reads (full scan)**

If you filter on a field that has no index entry yet — which can happen if you query before inserting any records with that field — the code falls back to loading all records and filtering in Python. This is O(n) and becomes noticeable above ~10,000 records.

**Writes**

Every write (insert, update, delete) does three things atomically:

1. Writes or updates the JSON blob in `records`
2. Removes stale entries from `field_index`
3. Inserts fresh entries into `field_index`

All three happen inside one SQLite transaction, so the index is always consistent with the data.

**Rule of thumb:**

| Operation | Speed |
|-----------|-------|
| `INSERT` | ~1–2 ms |
| `SELECT *` (1,000 records) | ~5 ms |
| `SELECT WHERE` (indexed) | < 1 ms |
| `SELECT WHERE` (unindexed, 10k records) | ~20–50 ms |
| `UPDATE` | ~2–3 ms |
| `DELETE` | ~1–2 ms |

Timings measured on a modern laptop with an SSD. Results vary by hardware.

---

## File organisation

```
project/
├── file_database.py      # Database engine — import this in your own code
├── cli.py                # Interactive CLI — run this directly
├── db.sqlite             # Your data (created automatically on first use)
└── README.md             # This file
```

**Inside the `.sqlite` file**, two tables are maintained automatically:

```
records
  id    TEXT PRIMARY KEY    — UUID generated on insert
  data  TEXT                — full record serialised as JSON

field_index
  field  TEXT               — field name  e.g. "role"
  value  TEXT               — field value e.g. "engineer"
  id     TEXT               — foreign key back to records.id
```

The `field_index` table has a composite B-tree index on `(field, value)` for fast lookups. It is kept in sync automatically — you never need to manage it manually.

If the index ever gets out of sync (e.g. after a manual edit to the file), run:

```
db> REBUILD
```

---

## Handling users

FileDatabase has **no built-in user system** — it is a single-user, local tool. All records belong to one implicit user: whoever runs the script.

**If you need multiple users**, the straightforward approach is to give each user their own database file:

```bash
python cli.py alice.sqlite
python cli.py bob.sqlite
```

**If you need user records** (e.g. storing a list of users in the database), treat `user` as just another field:

```
db> INSERT username=alice email=alice@example.com role=admin
db> INSERT username=bob   email=bob@example.com   role=viewer
db> SELECT WHERE role=admin
```

**If you need access control** — where different users should only see their own records — this is beyond the scope of FileDatabase. You would need to add an application layer that filters by a `user_id` field on every query, and ideally a proper authentication system. At that point, migrating to PostgreSQL with row-level security is worth considering.

---

## Using FileDatabase in your own code

```python
from file_database import FileDatabase

# Basic usage
db = FileDatabase("myapp.sqlite")

# With schema validation
db = FileDatabase("myapp.sqlite", schema={
    "name":  str,
    "email": str,
    "age":   int,
})

# Insert
record = db.insert({"name": "Alice", "email": "alice@example.com", "age": 30})

# Read all
records = db.read()

# Filter (uses index automatically)
admins = db.read(filters={"role": "admin"})

# Fetch one by ID
record = db.read(record_id="some-uuid-here")

# Update
db.update(record["id"], {"age": 31})

# Delete
db.delete(record["id"])

# Utilities
db.count()
db.index_stats()
db.rebuild_index()
db.clear()
db.close()
```

---

## CLI commands

| Command | Description |
|---------|-------------|
| `INSERT field=value ...` | Add a new record |
| `SELECT *` | Return all records |
| `SELECT WHERE field=value ...` | Filter records (uses index) |
| `SELECT id <id>` | Fetch one record by ID |
| `UPDATE <id> field=value ...` | Update fields on a record |
| `DELETE <id>` | Remove a record |
| `COUNT` | Show total number of records |
| `CLEAR` | Wipe all records (asks for confirmation) |
| `INDEXES` | Show index summary |
| `REBUILD` | Rebuild index from scratch |
| `SCHEMA` | Show active schema (if set) |
| `HELP` | Show command reference |
| `EXIT` / `QUIT` | Close the CLI |

**Tips:**
- Values that look like integers are stored as integers automatically (`level=3` stores `3`, not `"3"`)
- Use underscores for multi-word values: `role=senior_engineer`
- Copy IDs from `SELECT` output to use in `UPDATE` and `DELETE`
- Quoted values work too: `INSERT name="Alice Smith"`

---

## Compared to alternatives

| | FileDatabase | Raw SQLite | TinyDB | PostgreSQL |
|--|--|--|--|--|
| Setup | None | None | `pip install` | Server required |
| Storage | `.sqlite` file | `.sqlite` file | `.json` file | Server process |
| Atomic writes | Yes | Yes | No | Yes |
| Concurrent access | WAL mode | WAL mode | No | Full MVCC |
| Schema-free | Yes | No | Yes | No |
| Joins | No | Yes | No | Yes |
| Best for | Learning, CLI tools | Embedded apps | Rapid prototyping | Production apps |

---

## Requirements

- Python 3.10 or later
- No third-party dependencies — uses only the Python standard library (`sqlite3`, `uuid`, `json`, `os`, `shlex`)

---

## Licence

MIT — free to use, modify, and distribute.