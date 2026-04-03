"""
file_database.py  (v2 — with indexing)

Storage layout on disk:
  db.json          → list of all records
  db.index.json    → index file: { field: { value: [id, id, ...] } }

Index structure in memory (plain nested dicts):
  {
    "role":  { "engineer": ["id1", "id3"], "designer": ["id2"] },
    "level": { "3": ["id1"], "2": ["id2"], "5": ["id3"] }
  }

Values are always stored as strings in the index so the key is
hashable regardless of original type (int, bool, etc.).
"""

import json
import uuid
import os
from typing import Any


class FileDatabase:
    def __init__(self, filepath: str = "db.json"):
        """
        filepath: path to the main JSON data file.
        The index is stored automatically at <filepath>.index.json
        (e.g. "db.json" -> "db.index.json").
        """
        self.filepath = filepath
        self.index_path = filepath.replace(".json", ".index.json")

    # ------------------------------------------------------------------ #
    # Low-level disk I/O
    # ------------------------------------------------------------------ #

    def _load(self) -> list[dict]:
        """Load all records from disk. Returns [] if file missing."""
        if not os.path.exists(self.filepath):
            return []
        with open(self.filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, data: list[dict]) -> None:
        """Write all records to disk."""
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _load_index(self) -> dict:
        """
        Load the index from disk.
        Structure: { field_name: { str(value): [id, id, ...] } }
        Returns {} if the index file does not exist yet.
        """
        if not os.path.exists(self.index_path):
            return {}
        with open(self.index_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_index(self, index: dict) -> None:
        """Write the index to disk."""
        with open(self.index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------ #
    # Index management helpers
    # ------------------------------------------------------------------ #

    def _index_add(self, index: dict, record: dict) -> None:
        """
        Add one record's fields into the index (in-place).

        For each field in the record (except "id"), we do:
          index[field][str(value)].append(record["id"])

        We convert values to str so they work as dict keys in JSON.
        Example: level=3  ->  index["level"]["3"] = ["id1"]
        """
        record_id = record["id"]
        for field, value in record.items():
            if field == "id":
                continue
            key = str(value)
            if field not in index:
                index[field] = {}
            if key not in index[field]:
                index[field][key] = []
            index[field][key].append(record_id)

    def _index_remove(self, index: dict, record: dict) -> None:
        """
        Remove one record's contribution from the index (in-place).

        Walk every field the record had and pull its ID out of the
        corresponding bucket. Empty buckets are cleaned up.
        """
        record_id = record["id"]
        for field, value in record.items():
            if field == "id":
                continue
            key = str(value)
            if field in index and key in index[field]:
                index[field][key] = [
                    i for i in index[field][key] if i != record_id
                ]
                if not index[field][key]:
                    del index[field][key]
                if not index[field]:
                    del index[field]

    def rebuild_index(self) -> None:
        """
        Rebuild the entire index from scratch by scanning all records.

        Use this if you ever edit db.json manually, or to repair a
        corrupted index. Safe to call at any time.
        """
        data = self._load()
        index: dict = {}
        for record in data:
            self._index_add(index, record)
        self._save_index(index)
        print(f"[INDEX] Rebuilt. {len(data)} records, "
              f"{len(index)} indexed fields: {list(index.keys())}")

    def create_index(self, field: str) -> None:
        """
        Announce intent to index a field and check its current status.
        The index is maintained automatically on every write anyway;
        this method is just informational.
        """
        index = self._load_index()
        if field in index:
            ids = sum(len(v) for v in index[field].values())
            print(f"[INDEX] '{field}' already indexed ({ids} entries).")
        else:
            print(f"[INDEX] '{field}' not yet in index. "
                  "It will appear after the next insert/update.")

    # ------------------------------------------------------------------ #
    # INSERT
    # ------------------------------------------------------------------ #

    def insert(self, record: dict) -> dict:
        """
        Add a new record. Automatically updates the index.
        Returns the inserted record with its generated id.
        """
        data  = self._load()
        index = self._load_index()

        record = {**record, "id": str(uuid.uuid4())}
        data.append(record)

        self._index_add(index, record)
        self._save(data)
        self._save_index(index)

        print(f"[INSERT] id={record['id']}")
        return record

    # ------------------------------------------------------------------ #
    # READ  (uses index when possible)
    # ------------------------------------------------------------------ #

    def read(
        self,
        record_id: str | None = None,
        filters: dict[str, Any] | None = None,
        use_index: bool = True,
    ) -> "list[dict] | dict | None":
        """
        Fetch records.

        record_id   -> return one record (dict) or None.
        filters     -> return matching records. Uses the index when
                       use_index=True (default) and the filter field is
                       indexed; falls back to a full scan otherwise.
        (no args)   -> return all records.

        Multi-field filters use the index for the first field, then
        scan the smaller candidate list for remaining conditions.
        """
        if record_id is not None:
            for r in self._load():
                if r.get("id") == record_id:
                    return r
            return None

        if not filters:
            return self._load()

        index = self._load_index() if use_index else {}
        indexed_fields = [f for f in filters if f in index]

        if indexed_fields:
            primary_field = indexed_fields[0]
            primary_value = str(filters[primary_field])
            candidate_ids = set(
                index[primary_field].get(primary_value, [])
            )

            if not candidate_ids:
                return []

            candidates = [
                r for r in self._load()
                if r.get("id") in candidate_ids
            ]

            remaining = {k: v for k, v in filters.items()
                         if k != primary_field}
            if remaining:
                candidates = [
                    r for r in candidates
                    if all(r.get(k) == v for k, v in remaining.items())
                ]
            return candidates

        # No indexed field -> full scan
        data = self._load()
        return [
            r for r in data
            if all(r.get(k) == v for k, v in filters.items())
        ]

    # ------------------------------------------------------------------ #
    # UPDATE
    # ------------------------------------------------------------------ #

    def update(self, record_id: str, changes: dict) -> "dict | None":
        """
        Update fields on a record. Keeps the index consistent by
        removing the old entry and adding the updated one.
        """
        data  = self._load()
        index = self._load_index()
        changes.pop("id", None)

        for record in data:
            if record.get("id") == record_id:
                self._index_remove(index, record)
                record.update(changes)
                self._index_add(index, record)
                self._save(data)
                self._save_index(index)
                print(f"[UPDATE] id={record_id}")
                return record

        print(f"[UPDATE] id={record_id} not found.")
        return None

    # ------------------------------------------------------------------ #
    # DELETE
    # ------------------------------------------------------------------ #

    def delete(self, record_id: str) -> bool:
        """
        Delete a record and remove it from the index.
        """
        data  = self._load()
        index = self._load_index()

        for record in data:
            if record.get("id") == record_id:
                self._index_remove(index, record)
                data.remove(record)
                self._save(data)
                self._save_index(index)
                print(f"[DELETE] id={record_id}")
                return True

        print(f"[DELETE] id={record_id} not found.")
        return False

    # ------------------------------------------------------------------ #
    # Convenience
    # ------------------------------------------------------------------ #

    def count(self) -> int:
        return len(self._load())

    def clear(self) -> None:
        """Delete all records AND wipe the index."""
        self._save([])
        self._save_index({})
        print("[CLEAR] Records and index wiped.")

    def index_stats(self) -> None:
        """Print a summary of what is currently in the index."""
        index = self._load_index()
        if not index:
            print("[INDEX] Empty -- no data yet.")
            return
        print("[INDEX] Current index:")
        for field, buckets in index.items():
            total = sum(len(ids) for ids in buckets.values())
            print(f"  '{field}': {len(buckets)} unique values, "
                  f"{total} total entries")
            for value, ids in buckets.items():
                print(f"    {value!r:20s} -> {len(ids)} record(s)")


# ====================================================================== #
# Demo
# ====================================================================== #

if __name__ == "__main__":
    db = FileDatabase("demo_db.json")
    db.clear()

    print("\n--- INSERT 4 records ---")
    alice = db.insert({"name": "Alice", "role": "engineer", "level": 3})
    bob   = db.insert({"name": "Bob",   "role": "designer", "level": 2})
    carol = db.insert({"name": "Carol", "role": "engineer", "level": 5})
    dave  = db.insert({"name": "Dave",  "role": "manager",  "level": 4})

    print("\n--- Index stats after inserts ---")
    db.index_stats()

    print("\n--- Fast indexed search: role=engineer ---")
    engineers = db.read(filters={"role": "engineer"})
    print(f"Found: {[e['name'] for e in engineers]}")

    print("\n--- Fast indexed search: level=2 ---")
    junior = db.read(filters={"level": 2})
    print(f"Found: {[j['name'] for j in junior]}")

    print("\n--- UPDATE Bob: role -> senior designer ---")
    db.update(bob["id"], {"role": "senior designer", "level": 3})

    print("\n--- Index stats after update ---")
    db.index_stats()

    print("\n--- Verify old value is gone from index ---")
    designers = db.read(filters={"role": "designer"})
    print(f"'designer' results (should be 0): {designers}")
    senior = db.read(filters={"role": "senior designer"})
    print(f"'senior designer' results: {[r['name'] for r in senior]}")

    print("\n--- DELETE Carol ---")
    db.delete(carol["id"])

    print("\n--- Final index stats ---")
    db.index_stats()

    print(f"\nRecords remaining: {db.count()}")