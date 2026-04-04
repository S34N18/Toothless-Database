# Toothless-Database
# FileDatabase

A lightweight, file-backed database built in Python using SQLite. Designed for personal projects, CLI tools, and learning how databases work from the ground up. Ships with a full interactive command-line interface and a browser-based GUI built with Flask.

---

## What's included

```
project/
├── file_database.py        # Database engine — the core library
├── cli.py                  # Interactive command-line interface
├── flask_app/
│   ├── app.py              # Flask web GUI — browser interface
│   └── templates/          # HTML templates (base, index, insert, edit, schema)
├── .gitignore
└── README.md
```

Your data is stored in a single `.sqlite` file — no server, no installation beyond Python itself.

---

## Quick start

### Option 1 — CLI

```bash
python cli.py                  # uses db.sqlite by default
python cli.py mydata.sqlite    # or point at a custom file
```

```
db> INSERT name=Alice role=engineer level=3
db> SELECT *
db> SELECT WHERE role=engineer
db> SELECT * ORDER BY level DESC LIMIT 10
db> UPDATE <id> level=5
db> DELETE <id>
db> EXIT
```

### Option 2 — Flask GUI

```bash
# First time: create a virtual environment
python3 -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate
pip install flask

# Run the GUI
cd flask_app
python app.py                  # opens at http://127.0.0.1:5000
python app.py mydata.sqlite    # or point at a custom file
```

Then open `http://127.0.0.1:5000` in your browser.

> **Note:** Activate the virtual environment (`source venv/bin/activate`) every time
> you open a new terminal to work on this project.

---

## The Flask GUI

The browser interface gives you full database management without typing commands.

| Page | URL | What it does |
|------|-----|--------------|
| Records | `/` | Paginated table with search, sort, and inline edit/delete |
| Insert | `/insert` | Form to add new records with quick-fill examples |
| Edit | `/edit/<id>` | Pre-filled form to update any field on a record |
| Schema | `/schema` | Column layout, type tags, and blob field index stats |
| JSON API | `/api/records` | All records as JSON — useful for scripting or external tools |

**Records page features:**
- Click any column header to sort ascending/descending
- Filter bar: type a `field` and `value` to search (uses the index automatically)
- Pagination: 20 records per page with page navigation
- Delete shows a confirmation modal before committing

**Insert / Edit form syntax** — one `field=value` pair per line:
```
name=Alice
role=engineer
level=3
country=Kenya
```
Numbers are detected and stored as integers automatically.

---

## Architecture — hybrid design

FileDatabase uses a **hybrid storage model** introduced in v4:

```
records table
┌─────────────┬──────────┬──────────────────────────────┐
│ id          │ TEXT PK  │ UUID, always present         │
│ created_at  │ TEXT     │ ISO timestamp, auto-set      │
│ <col> ...   │ varies   │ declared schema columns      │  ← core fields
│ data        │ TEXT     │ JSON blob for extra fields   │  ← blob fields
└─────────────┴──────────┴──────────────────────────────┘
```

- **Core fields** (declared in `schema=`) → real SQLite columns. Native SQL queries, `ORDER BY`, range filters, and indexes all work on these.
- **Blob fields** (anything not in schema) → stored in the `data` JSON column and indexed via a companion `field_index` table. Flexible — no schema change needed.

This mirrors how PostgreSQL's `JSONB` pattern works in production: structured columns for things you know, a flexible blob for everything else.

---

## Scalability

FileDatabase is designed for **small to medium datasets** — personal tools, prototypes, and single-user applications.

| Dataset size | Performance |
|---|---|
| Up to ~10,000 records | Fast — sub-millisecond indexed reads |
| 10,000 – 100,000 records | Usable — indexed queries stay quick, full scans slow |
| 100,000+ records | Not recommended — consider PostgreSQL |

**What helps it scale:**

- Core schema columns get native SQLite B-tree indexes — O(log n) lookups.
- Blob fields are indexed via `field_index`, also B-tree — avoids full scans for common queries.
- WAL mode (`PRAGMA journal_mode=WAL`) allows concurrent reads and writes without blocking.
- `ORDER BY`, `LIMIT`, and `OFFSET` are pushed down to SQLite natively on core columns — no Python-side sorting or slicing.

**Where it won't scale:**

- No connection pooling or multi-process coordination. For web apps with many concurrent users, use PostgreSQL.
- Blob fields cannot be sorted natively — `ORDER BY` only works on declared schema columns.

---

## Security

FileDatabase is a **local, single-user tool**. It has no network interface, no authentication layer, and no concept of users or permissions — by design.

**What is protected:**

- **Atomic writes** — every write runs inside a SQLite transaction. Crashes mid-write roll back automatically. No half-written records.
- **WAL journaling** — a power cut during a write leaves the database intact. SQLite recovers on the next open.
- **Schema validation** — if you pass a `schema=` argument, every insert and update is type-checked before touching the database. Bad data raises a clear `TypeError` before anything is written.
- **Parameterised queries** — all SQL uses `?` placeholders, not string formatting. SQL injection via the CLI or GUI is not possible.

**What is not protected:**

- **No encryption** — the `.sqlite` file is readable by anyone with filesystem access. Do not store passwords, tokens, or sensitive personal data without encrypting the file first (e.g. with `SQLCipher`).
- **No access control** — anyone who can run Python and access the file can read or modify all records.
- **Flask runs in debug mode** by default — suitable for local use only. Before exposing the GUI to a network, set `debug=False` and run behind a proper WSGI server like `gunicorn`.

**Recommended use:** local scripts, developer tooling, personal data that does not need to be shared over a network.

---

## Speed

**Indexed reads (fast path)**

When you query a core column or an indexed blob field, the database:

1. Looks up the value in the B-tree index — O(log n)
2. Retrieves the matching record IDs — typically a small set
3. Fetches only those rows by primary key — O(1) per row

A `SELECT WHERE role=engineer` on 50,000 records returns in under 1 ms if the result set is small.

**Full scan fallback**

If you filter on a blob field with no `field_index` entry yet, the code falls back to loading all records and filtering in Python. O(n) — becomes noticeable above ~10,000 records.

**Rule of thumb:**

| Operation | Speed |
|---|---|
| `INSERT` | ~1–2 ms |
| `SELECT *` (1,000 records) | ~5 ms |
| `SELECT WHERE` (core column) | < 1 ms |
| `SELECT WHERE` (indexed blob field) | < 1 ms |
| `SELECT WHERE` (unindexed, 10k records) | ~20–50 ms |
| `UPDATE` | ~2–3 ms |
| `DELETE` | ~1–2 ms |

Timings on a modern laptop with an SSD. Results vary by hardware.

---

## File organisation

```
project/
├── file_database.py        # Database engine — import this in your own code
├── cli.py                  # Interactive CLI
├── flask_app/
│   ├── app.py              # Flask routes and request handling
│   ├── file_database.py    # Copy of the engine (for self-contained deployment)
│   └── templates/
│       ├── base.html       # Sidebar, topbar, shared design system
│       ├── index.html      # Records table, search, pagination
│       ├── insert.html     # Insert form
│       ├── edit.html       # Edit form + delete
│       └── schema.html     # Column layout and index stats
├── venv/                   # Virtual environment (never commit this)
├── .gitignore
└── README.md
```

**Inside the `.sqlite` file**, three tables are maintained automatically:

```
_meta
  key    TEXT PK   — internal versioning
  value  TEXT

records
  id          TEXT PK   — UUID generated on insert
  created_at  TEXT      — ISO 8601 UTC timestamp, auto-set
  <col> ...             — declared schema columns (one per field in schema=)
  data        TEXT      — JSON blob for all extra fields

field_index
  field  TEXT   — field name  e.g. "country"
  value  TEXT   — field value e.g. "Kenya"
  id     TEXT   — foreign key → records.id  (ON DELETE CASCADE)
```

The `field_index` table has a composite B-tree index on `(field, value)`. It is kept in sync automatically on every insert, update, and delete — you never manage it manually.

---

## Automatic migration

When you open an older database file with a newer version of `FileDatabase`, the engine detects the old layout and upgrades it automatically — preserving all existing records and their IDs. No manual steps needed.

| Version | Change |
|---|---|
| v1–v2 | JSON + separate `.index.json` file |
| v3 | Single `.sqlite` file, blob-only storage |
| v4 (current) | Hybrid: core columns + JSON blob |

---

## Handling users

FileDatabase has no built-in user system — it is a single-user, local tool.

**If you need multiple users**, give each one their own database file:

```bash
python cli.py alice.sqlite
python cli.py bob.sqlite

# Or with the GUI:
python app.py alice.sqlite
python app.py bob.sqlite
```

**If you need user records** (storing a list of users as data), treat `user` as just another field:

```
db> INSERT username=alice email=alice@example.com role=admin
db> INSERT username=bob   email=bob@example.com   role=viewer
db> SELECT WHERE role=admin
```

**If you need access control** — where users should only see their own records — this is beyond the scope of FileDatabase. You would need an application layer that filters by a `user_id` field on every query. At that point, migrating to PostgreSQL with row-level security is worth considering.

---

## Using FileDatabase in your own code

```python
from file_database import FileDatabase

# Basic — any fields accepted
db = FileDatabase("myapp.sqlite")

# With schema: declared fields become real SQLite columns
# Extra fields still work — they go into the JSON blob automatically
db = FileDatabase("myapp.sqlite", schema={
    "name":  str,
    "role":  str,
    "level": int,
})

# Only index specific blob fields (saves space on wide records)
db = FileDatabase("myapp.sqlite", schema={"name": str, "role": str},
                  indexed_fields=["role"])

# Insert — core fields → columns, extras → blob
record = db.insert({"name": "Alice", "role": "engineer", "level": 3,
                    "country": "Kenya"})   # country goes to blob

# Read all
records = db.read()

# Filter by core column (native SQL)
engineers = db.read(filters={"role": "engineer"})

# Filter by blob field (field_index lookup)
kenyan = db.read(filters={"country": "Kenya"})

# Sort and paginate
page1 = db.read(order_by="level", descending=True, limit=20, offset=0)

# Fetch one by ID
record = db.read(record_id="some-uuid-here")

# Update — only listed fields change
db.update(record["id"], {"level": 5, "city": "Nairobi"})

# Delete
db.delete(record["id"])

# Utilities
db.count()
db.index_stats()
db.schema_info()
db.rebuild_index()
db.clear()
db.close()
```

---

## CLI commands

| Command | Description |
|---|---|
| `INSERT field=value ...` | Add a new record |
| `SELECT *` | Return all records |
| `SELECT * ORDER BY field [DESC] [LIMIT n] [OFFSET n]` | Sorted and paginated |
| `SELECT WHERE field=value ...` | Filter records (uses index) |
| `SELECT WHERE ... ORDER BY field DESC LIMIT n` | Filter + sort + paginate |
| `SELECT id <id>` | Fetch one record by ID |
| `UPDATE <id> field=value ...` | Update fields on a record |
| `DELETE <id>` | Remove a record |
| `COUNT` | Total number of records |
| `CLEAR` | Wipe all records (asks for confirmation) |
| `INDEXES` | Show index summary (core columns + blob fields) |
| `SCHEMA` | Show table column layout |
| `REBUILD` | Rebuild blob field_index from scratch |
| `HELP` | Show command reference |
| `EXIT` / `QUIT` | Close the CLI |

**Tips:**
- Numbers are stored as integers automatically: `level=3` stores `3`, not `"3"`
- Use underscores for multi-word values: `role=senior_engineer`
- Quoted values work: `INSERT name="Alice Smith"`
- Copy IDs from `SELECT` output to use in `UPDATE` and `DELETE`
- `ORDER BY` works on declared schema columns and `created_at`

---

## Compared to alternatives

| | FileDatabase | Raw SQLite | TinyDB | PostgreSQL | MongoDB |
|---|---|---|---|---|---|
| Setup | None | None | `pip install` | Server required | Server required |
| Storage | `.sqlite` file | `.sqlite` file | `.json` file | Server process | Server process |
| Atomic writes | Yes | Yes | No | Yes | Yes |
| Concurrent access | WAL mode | WAL mode | No | Full MVCC | Full |
| Schema-free | Hybrid | No | Yes | No | Yes |
| Joins | No | Yes | No | Yes | Manual |
| ORDER BY / LIMIT | Yes (core cols) | Yes | No | Yes | Yes |
| Browser GUI | Yes (Flask) | No | No | pgAdmin etc. | Compass etc. |
| Best for | Learning, CLI tools, personal apps | Embedded apps | Rapid prototyping | Production apps | Flexible docs |

---

## Requirements

**Database engine and CLI:**
- Python 3.10 or later
- No third-party dependencies — uses only the standard library (`sqlite3`, `uuid`, `json`, `os`, `shlex`, `datetime`)

**Flask GUI:**
- Python 3.10 or later
- `flask` — install inside a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install flask
```

---

## .gitignore

```
venv/
__pycache__/
*.pyc
*.sqlite
.env
```

---

## Licence

MIT — free to use, modify, and distribute.