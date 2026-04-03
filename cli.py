"""
cli.py  (v4 — hybrid database)

Commands:
    INSERT field=value [field=value ...]
    SELECT *
    SELECT WHERE field=value [field=value ...] [ORDER BY field] [DESC] [LIMIT n] [OFFSET n]
    SELECT id <record_id>
    UPDATE <id> field=value [field=value ...]
    DELETE <id>
    COUNT
    CLEAR
    INDEXES
    SCHEMA
    REBUILD
    HELP
    EXIT / QUIT
"""

import sys
import shlex
from file_database import FileDatabase


def _hr(char="─", width=60):
    return char * width


def _print_record(record: dict, indent=2) -> None:
    pad = " " * indent
    print(f"{pad}id         : {record.get('id', '?')}")
    print(f"{pad}created_at : {record.get('created_at', '?')}")
    for k, v in record.items():
        if k in ("id", "created_at"):
            continue
        print(f"{pad}{k:<12}: {v}")


def _print_records(records: list[dict]) -> None:
    if not records:
        print("  (no results)")
        return
    for i, r in enumerate(records):
        if i > 0:
            print(f"  {'·' * 40}")
        _print_record(r)
    print(f"\n  {len(records)} record(s) found.")


def _coerce(value: str):
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _parse_pairs(tokens: list[str]) -> dict:
    result = {}
    for token in tokens:
        if "=" not in token:
            raise ValueError(f"Expected field=value, got: {token!r}")
        field, _, raw = token.partition("=")
        result[field.strip()] = _coerce(raw.strip())
    return result


# ------------------------------------------------------------------ #
# Command handlers
# ------------------------------------------------------------------ #

def cmd_insert(db, tokens):
    if not tokens:
        print("  Error: INSERT needs at least one field=value pair.")
        return
    try:
        record = _parse_pairs(tokens)
    except ValueError as e:
        print(f"  Error: {e}")
        return
    result = db.insert(record)
    print("  Inserted:")
    _print_record(result)


def cmd_select(db, tokens):
    """
    SELECT *
    SELECT id <id>
    SELECT WHERE f=v [f=v ...] [ORDER BY field] [DESC] [LIMIT n] [OFFSET n]
    """
    if not tokens or tokens[0] == "*":
        # Check for ORDER BY / LIMIT / OFFSET after *
        rest = tokens[1:] if tokens else []
        order_by, descending, limit, offset = _parse_select_opts(rest)
        records = db.read(order_by=order_by, descending=descending,
                          limit=limit, offset=offset)
        _print_records(records)
        return

    if tokens[0].upper() == "ID" and len(tokens) >= 2:
        record = db.read(record_id=tokens[1])
        if record:
            _print_record(record)
        else:
            print(f"  No record with id={tokens[1]!r}")
        return

    if tokens[0].upper() == "WHERE":
        # Split filter tokens from ORDER BY / LIMIT / OFFSET
        filter_tokens, opt_tokens = _split_select_tokens(tokens[1:])
        try:
            filters = _parse_pairs(filter_tokens)
        except ValueError as e:
            print(f"  Error: {e}")
            return
        order_by, descending, limit, offset = _parse_select_opts(opt_tokens)
        records = db.read(filters=filters, order_by=order_by,
                          descending=descending, limit=limit, offset=offset)
        _print_records(records)
        return

    print("  Error: Unknown SELECT syntax.")
    print("  Use: SELECT *  |  SELECT WHERE f=v [ORDER BY f] [DESC] [LIMIT n] [OFFSET n]  |  SELECT id <id>")


def _split_select_tokens(tokens: list[str]):
    """Split WHERE field tokens from ORDER BY / LIMIT / OFFSET tokens."""
    keywords = {"ORDER", "LIMIT", "OFFSET"}
    for i, t in enumerate(tokens):
        if t.upper() in keywords:
            return tokens[:i], tokens[i:]
    return tokens, []


def _parse_select_opts(tokens: list[str]):
    """Parse ORDER BY field [DESC] LIMIT n OFFSET n from token list."""
    order_by   = None
    descending = False
    limit      = None
    offset     = 0

    i = 0
    while i < len(tokens):
        t = tokens[i].upper()
        if t == "ORDER" and i + 1 < len(tokens) and tokens[i+1].upper() == "BY":
            if i + 2 < len(tokens):
                order_by = tokens[i+2]
                i += 3
                if i < len(tokens) and tokens[i].upper() == "DESC":
                    descending = True
                    i += 1
            else:
                i += 2
        elif t == "DESC":
            descending = True
            i += 1
        elif t == "LIMIT" and i + 1 < len(tokens):
            try:
                limit = int(tokens[i+1])
            except ValueError:
                pass
            i += 2
        elif t == "OFFSET" and i + 1 < len(tokens):
            try:
                offset = int(tokens[i+1])
            except ValueError:
                pass
            i += 2
        else:
            i += 1

    return order_by, descending, limit, offset


def cmd_update(db, tokens):
    if len(tokens) < 2:
        print("  Error: UPDATE needs an id and at least one field=value pair.")
        return
    record_id = tokens[0]
    try:
        changes = _parse_pairs(tokens[1:])
    except ValueError as e:
        print(f"  Error: {e}")
        return
    result = db.update(record_id, changes)
    if result:
        print("  Updated:")
        _print_record(result)
    else:
        print(f"  No record with id={record_id!r}")


def cmd_delete(db, tokens):
    if not tokens:
        print("  Error: DELETE needs a record id.")
        return
    ok = db.delete(tokens[0])
    if ok:
        print(f"  Deleted record id={tokens[0]!r}")
    else:
        print(f"  No record with id={tokens[0]!r}")


def cmd_clear(db):
    confirm = input("  Type YES to wipe all records: ").strip()
    if confirm == "YES":
        db.clear()
    else:
        print("  Aborted.")


def cmd_help():
    print(f"""
  {_hr()}
  Commands
  {_hr()}
  INSERT field=value [field=value ...]
      Add a record. Core schema fields → real columns. Others → blob.
      Example: INSERT name=Alice role=engineer level=3 country=Kenya

  SELECT *  [ORDER BY field] [DESC] [LIMIT n] [OFFSET n]
      Return all records, optionally sorted and paginated.
      Example: SELECT * ORDER BY level DESC LIMIT 5

  SELECT WHERE field=value [field=value ...] [ORDER BY field] [DESC] [LIMIT n] [OFFSET n]
      Filter records. Core fields use native SQL; blob fields use index.
      Example: SELECT WHERE role=engineer ORDER BY level DESC
      Example: SELECT WHERE country=Kenya LIMIT 10 OFFSET 20

  SELECT id <record_id>
      Fetch one record by ID.

  UPDATE <id> field=value [field=value ...]
      Overwrite specific fields. Others are untouched.
      Example: UPDATE abc-123 level=5 country=Uganda

  DELETE <id>        Remove a record.
  COUNT              Total number of records.
  CLEAR              Wipe everything (asks for confirmation).
  INDEXES            Show index summary (core + blob).
  SCHEMA             Show table column layout.
  REBUILD            Rebuild blob field_index from scratch.
  HELP               Show this message.
  EXIT / QUIT        Close the CLI.
  {_hr()}
  Notes
  {_hr()}
  - Core schema fields are fast: native SQL, ORDER BY, ranges work.
  - Blob fields are flexible: any name, indexed via field_index.
  - Numbers are auto-detected: level=3 stores integer 3.
  - Quoted values: INSERT name="Alice Smith" (shlex handles it).
  - created_at is set automatically — you can ORDER BY it.
  {_hr()}""")


# ------------------------------------------------------------------ #
# REPL
# ------------------------------------------------------------------ #

def run(db: FileDatabase) -> None:
    count = db.count()
    print(_hr("═"))
    print(f"  FileDatabase v4  —  {db.filepath}  ({count} records)")
    print(f"  Type HELP for commands, EXIT to quit.")
    print(_hr("═"))

    while True:
        try:
            raw = input("\ndb> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye.")
            break

        if not raw:
            continue

        try:
            parts = shlex.split(raw)
        except ValueError as e:
            print(f"  Parse error: {e}")
            continue

        command = parts[0].upper()
        tokens  = parts[1:]
        print()

        if command == "INSERT":
            cmd_insert(db, tokens)
        elif command == "SELECT":
            cmd_select(db, tokens)
        elif command == "UPDATE":
            cmd_update(db, tokens)
        elif command == "DELETE":
            cmd_delete(db, tokens)
        elif command == "COUNT":
            print(f"  {db.count()} record(s).")
        elif command == "CLEAR":
            cmd_clear(db)
        elif command == "INDEXES":
            db.index_stats()
        elif command == "SCHEMA":
            db.schema_info()
        elif command == "REBUILD":
            db.rebuild_index()
        elif command == "HELP":
            cmd_help()
        elif command in ("EXIT", "QUIT"):
            print("  Goodbye.")
            break
        else:
            print(f"  Unknown command: {command!r}. Type HELP.")


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "db.sqlite"
    db = FileDatabase(filepath)
    run(db)