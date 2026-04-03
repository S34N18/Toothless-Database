"""
cli.py — interactive command-line interface for FileDatabase

Usage:
    python cli.py             (uses db.json by default)
    python cli.py mydata.json (use a custom file)

Commands:
    INSERT field=value field=value ...
    SELECT *
    SELECT WHERE field=value [field=value ...]
    SELECT id <record_id>
    UPDATE <id> field=value [field=value ...]
    DELETE <id>
    COUNT
    CLEAR
    INDEXES
    REBUILD
    HELP
    EXIT  (or QUIT)
"""

import sys
import shlex
from file_database import FileDatabase


# ------------------------------------------------------------------ #
# Pretty-printing helpers
# ------------------------------------------------------------------ #

def _hr(char: str = "─", width: int = 60) -> str:
    return char * width


def _print_record(record: dict, indent: int = 2) -> None:
    pad = " " * indent
    id_val = record.get("id", "?")
    fields = {k: v for k, v in record.items() if k != "id"}
    print(f"{pad}id  : {id_val}")
    for k, v in fields.items():
        print(f"{pad}{k:<10}: {v}")


def _print_records(records: list[dict]) -> None:
    if not records:
        print("  (no results)")
        return
    for i, r in enumerate(records):
        if i > 0:
            print(f"  {'·' * 40}")
        _print_record(r)
    print(f"\n  {len(records)} record(s) found.")


# ------------------------------------------------------------------ #
# Value coercion
# Tries int, then float, then leaves as string.
# ------------------------------------------------------------------ #

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


# ------------------------------------------------------------------ #
# Parse  field=value  pairs from a list of tokens
# ------------------------------------------------------------------ #

def _parse_pairs(tokens: list[str]) -> dict:
    """
    Turn ["name=Alice", "level=3"] into {"name": "Alice", "level": 3}.
    Raises ValueError if any token is not in field=value form.
    """
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

def cmd_insert(db: FileDatabase, tokens: list[str]) -> None:
    """INSERT field=value [field=value ...]"""
    if not tokens:
        print("  Error: INSERT needs at least one field=value pair.")
        print("  Example: INSERT name=Alice role=engineer")
        return
    try:
        record = _parse_pairs(tokens)
    except ValueError as e:
        print(f"  Error: {e}")
        return
    result = db.insert(record)
    print(f"  Inserted 1 record.")
    _print_record(result)


def cmd_select(db: FileDatabase, tokens: list[str]) -> None:
    """
    SELECT *
    SELECT WHERE field=value [field=value ...]
    SELECT id <record_id>
    """
    if not tokens or tokens[0] == "*":
        records = db.read()
        _print_records(records)
        return

    if tokens[0].upper() == "ID" and len(tokens) == 2:
        record = db.read(record_id=tokens[1])
        if record:
            _print_record(record)
        else:
            print(f"  No record with id={tokens[1]!r}")
        return

    if tokens[0].upper() == "WHERE":
        try:
            filters = _parse_pairs(tokens[1:])
        except ValueError as e:
            print(f"  Error: {e}")
            return
        records = db.read(filters=filters)
        _print_records(records)
        return

    print("  Error: Unknown SELECT syntax.")
    print("  Use: SELECT *  |  SELECT WHERE field=value  |  SELECT id <id>")


def cmd_update(db: FileDatabase, tokens: list[str]) -> None:
    """UPDATE <id> field=value [field=value ...]"""
    if len(tokens) < 2:
        print("  Error: UPDATE needs an id and at least one field=value pair.")
        print("  Example: UPDATE abc-123 role=manager")
        return
    record_id = tokens[0]
    try:
        changes = _parse_pairs(tokens[1:])
    except ValueError as e:
        print(f"  Error: {e}")
        return
    result = db.update(record_id, changes)
    if result:
        print("  Updated successfully:")
        _print_record(result)
    else:
        print(f"  No record with id={record_id!r}")


def cmd_delete(db: FileDatabase, tokens: list[str]) -> None:
    """DELETE <id>"""
    if not tokens:
        print("  Error: DELETE needs a record id.")
        print("  Example: DELETE abc-123")
        return
    record_id = tokens[0]
    ok = db.delete(record_id)
    if ok:
        print(f"  Deleted record id={record_id!r}")
    else:
        print(f"  No record with id={record_id!r}")


def cmd_count(db: FileDatabase) -> None:
    n = db.count()
    print(f"  {n} record(s) in database.")


def cmd_clear(db: FileDatabase) -> None:
    confirm = input("  Type YES to wipe all records and the index: ").strip()
    if confirm == "YES":
        db.clear()
        print("  Database cleared.")
    else:
        print("  Aborted.")


def cmd_indexes(db: FileDatabase) -> None:
    db.index_stats()


def cmd_rebuild(db: FileDatabase) -> None:
    db.rebuild_index()


def cmd_help() -> None:
    print(f"""
  {_hr()}
  Commands
  {_hr()}
  INSERT field=value [field=value ...]
      Add a new record. Fields can be any name/value pairs.
      Example: INSERT name=Alice role=engineer level=3

  SELECT *
      Show all records.

  SELECT WHERE field=value [field=value ...]
      Filter records. Uses the index when available.
      Example: SELECT WHERE role=engineer
      Example: SELECT WHERE role=engineer level=3

  SELECT id <record_id>
      Fetch one record by its id.

  UPDATE <id> field=value [field=value ...]
      Change fields on a record. Only listed fields are overwritten.
      Example: UPDATE abc-123 role=manager level=5

  DELETE <id>
      Remove a record permanently.
      Example: DELETE abc-123

  COUNT     Show total number of records.
  CLEAR     Wipe all records and the index (asks for confirmation).
  INDEXES   Show a summary of the current index.
  REBUILD   Rebuild the index from scratch (use if index seems wrong).
  HELP      Show this message.
  EXIT      Quit. (QUIT also works.)
  {_hr()}
  Notes
  {_hr()}
  - Values that look like numbers are stored as numbers (3, not "3").
  - Use underscores for values with spaces: role=senior_engineer
    then query them back as: SELECT WHERE role=senior_engineer
  - IDs are UUIDs — copy them from SELECT output for UPDATE/DELETE.
  {_hr()}""")


# ------------------------------------------------------------------ #
# Main REPL
# ------------------------------------------------------------------ #

def run(db: FileDatabase) -> None:
    db_name = db.filepath
    count = db.count()
    print(_hr("═"))
    print(f"  FileDatabase CLI  —  {db_name}  ({count} records)")
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

        # shlex.split handles quoted strings like name="Alice Smith"
        try:
            parts = shlex.split(raw)
        except ValueError as e:
            print(f"  Parse error: {e}")
            continue

        command = parts[0].upper()
        tokens  = parts[1:]

        print()  # breathing room before output

        if command == "INSERT":
            cmd_insert(db, tokens)
        elif command == "SELECT":
            cmd_select(db, tokens)
        elif command == "UPDATE":
            cmd_update(db, tokens)
        elif command == "DELETE":
            cmd_delete(db, tokens)
        elif command == "COUNT":
            cmd_count(db)
        elif command == "CLEAR":
            cmd_clear(db)
        elif command == "INDEXES":
            cmd_indexes(db)
        elif command == "REBUILD":
            cmd_rebuild(db)
        elif command == "HELP":
            cmd_help()
        elif command == "SCHEMA":
            if db.schema:
                print("  Active schema:")
                for field, typ in db.schema.items():
                    print(f"    {field:<16} {typ.__name__}")
            else:
                print("  No schema defined (any fields accepted).")
        elif command in ("EXIT", "QUIT"):
            print("  Goodbye.")
            break
        else:
            print(f"  Unknown command: {command!r}. Type HELP to see all commands.")


# ------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "db.sqlite"
    db = FileDatabase(filepath)
    run(db)