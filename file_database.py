"""
file_database.py
A simple file-backed database using JSON for persistent storage.
Supports insert, read, update, and delete (CRUD) operations.
"""

import json
import uuid
import os
from typing import Any


class FileDatabase:
    def __init__(self, filepath: str = "db.json"):
        """
        Initialize the database.
        filepath: path to the JSON file used for storage.
        The file is created automatically on first write.
        """
        self.filepath = filepath

    # ------------------------------------------------------------------ #
    # Private helpers: load from disk and save to disk
    # ------------------------------------------------------------------ #

    def _load(self) -> list[dict]:
        """Read all records from the JSON file.
        Returns an empty list if the file doesn't exist yet."""
        if not os.path.exists(self.filepath):
            return []
        with open(self.filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, data: list[dict]) -> None:
        """Write the full list of records back to the JSON file."""
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------ #
    # INSERT — add a new record
    # ------------------------------------------------------------------ #

    def insert(self, record: dict) -> dict:
        """
        Insert a new record into the database.

        - Automatically assigns a unique 'id' field (UUID).
        - If you supply your own 'id', it will be overwritten.

        Returns the inserted record (including its generated id).
        """
        data = self._load()

        # Give the record a unique ID
        record = {**record, "id": str(uuid.uuid4())}

        data.append(record)
        self._save(data)

        print(f"[INSERT] Added record with id={record['id']}")
        return record

    # ------------------------------------------------------------------ #
    # READ — fetch one or many records
    # ------------------------------------------------------------------ #

    def read(
        self,
        record_id: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[dict] | dict | None:
        """
        Read records from the database.

        - record_id: if given, return the single matching record (or None).
        - filters:   a dict of {field: value} pairs; returns only records
                     where ALL conditions match (simple equality check).
        - No arguments: return all records.
        """
        data = self._load()

        # Return one specific record by ID
        if record_id is not None:
            for record in data:
                if record.get("id") == record_id:
                    return record
            return None  # Not found

        # Return records matching all filter conditions
        if filters:
            return [
                r for r in data
                if all(r.get(k) == v for k, v in filters.items())
            ]

        # Return everything
        return data

    # ------------------------------------------------------------------ #
    # UPDATE — modify an existing record
    # ------------------------------------------------------------------ #

    def update(self, record_id: str, changes: dict) -> dict | None:
        """
        Update a record by its id.

        - changes: a dict of fields to overwrite. Fields not mentioned
                   are left untouched.
        - The 'id' field cannot be changed (it's silently removed from
                   changes if you include it).

        Returns the updated record, or None if the id wasn't found.
        """
        data = self._load()
        changes.pop("id", None)  # Protect the ID from being overwritten

        for record in data:
            if record.get("id") == record_id:
                record.update(changes)
                self._save(data)
                print(f"[UPDATE] Updated record id={record_id}")
                return record

        print(f"[UPDATE] Record id={record_id} not found.")
        return None

    # ------------------------------------------------------------------ #
    # DELETE — remove a record
    # ------------------------------------------------------------------ #

    def delete(self, record_id: str) -> bool:
        """
        Delete a record by its id.

        Returns True if the record was found and deleted, False otherwise.
        """
        data = self._load()
        new_data = [r for r in data if r.get("id") != record_id]

        if len(new_data) == len(data):
            print(f"[DELETE] Record id={record_id} not found.")
            return False

        self._save(new_data)
        print(f"[DELETE] Deleted record id={record_id}")
        return True

    # ------------------------------------------------------------------ #
    # Convenience
    # ------------------------------------------------------------------ #

    def count(self) -> int:
        """Return the total number of records."""
        return len(self._load())

    def clear(self) -> None:
        """Delete all records (the file remains, but becomes empty)."""
        self._save([])
        print("[CLEAR] All records deleted.")


# ====================================================================== #
# Demo — run this file directly to see it in action
# ====================================================================== #

if __name__ == "__main__":
    db = FileDatabase("demo_db.json")
    db.clear()  # Start fresh for the demo

    print("\n--- INSERT ---")
    alice = db.insert({"name": "Alice", "role": "engineer", "level": 3})
    bob   = db.insert({"name": "Bob",   "role": "designer", "level": 2})
    carol = db.insert({"name": "Carol", "role": "engineer", "level": 5})

    print("\n--- READ ALL ---")
    for person in db.read():
        print(person)

    print("\n--- READ ONE ---")
    print(db.read(record_id=alice["id"]))

    print("\n--- READ WITH FILTER ---")
    engineers = db.read(filters={"role": "engineer"})
    print(f"Engineers: {[e['name'] for e in engineers]}")

    print("\n--- UPDATE ---")
    db.update(bob["id"], {"level": 4, "role": "senior designer"})
    print(db.read(record_id=bob["id"]))

    print("\n--- DELETE ---")
    db.delete(carol["id"])
    print(f"Records remaining: {db.count()}")

    print("\n--- FINAL STATE ---")
    for person in db.read():
        print(person)