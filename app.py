"""
app.py — Flask GUI for FileDatabase

Run:
    python app.py
    python app.py mydata.sqlite

Opens at http://127.0.0.1:5000
"""

import sys
import os
import json
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify

sys.path.insert(0, os.path.dirname(__file__))
from file_database import FileDatabase

app = Flask(__name__)
app.secret_key = "filedb-flask-gui"

# ── initialise database ───────────────────────────────────────────────
DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "data.sqlite"
db = FileDatabase(DB_PATH)


def _all_keys(records):
    """Collect every field key seen across all records (for table headers)."""
    keys = []
    seen = set()
    priority = ["name", "role", "level"]   # show these first if present
    for p in priority:
        for r in records:
            if p in r and p not in seen:
                keys.append(p)
                seen.add(p)
    for r in records:
        for k in r:
            if k not in seen and k not in ("id", "created_at", "data"):
                keys.append(k)
                seen.add(k)
    return keys


# ── routes ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    search_field = request.args.get("field", "").strip()
    search_value = request.args.get("value", "").strip()
    order_by     = request.args.get("order_by", "created_at")
    descending   = request.args.get("desc", "1") == "1"
    page         = max(1, int(request.args.get("page", 1)))
    per_page     = 20

    # Build filters
    filters = None
    if search_field and search_value:
        # coerce to int if possible
        try:
            v = int(search_value)
        except ValueError:
            v = search_value
        filters = {search_field: v}

    # Validate order_by against core cols
    core_cols = set(db._core_cols) | {"id", "created_at"}
    if order_by not in core_cols:
        order_by = "created_at"

    records = db.read(
        filters=filters,
        order_by=order_by,
        descending=descending,
        limit=per_page,
        offset=(page - 1) * per_page,
    )

    # Total count for pagination (with filters)
    if filters:
        total = len(db.read(filters=filters))
    else:
        total = db.count()

    total_pages = max(1, (total + per_page - 1) // per_page)
    col_keys    = _all_keys(db.read(limit=50) or [])

    return render_template(
        "index.html",
        records=records,
        col_keys=col_keys,
        total=total,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        search_field=search_field,
        search_value=search_value,
        order_by=order_by,
        descending=descending,
        db_path=DB_PATH,
        core_cols=sorted(core_cols - {"id", "created_at"}),
    )


@app.route("/insert", methods=["GET", "POST"])
def insert():
    if request.method == "POST":
        raw = request.form.get("fields", "").strip()
        record = {}
        errors = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            if "=" not in line:
                errors.append(f"Invalid line (no '='): {line!r}")
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            try:
                record[key] = int(val)
            except ValueError:
                try:
                    record[key] = float(val)
                except ValueError:
                    record[key] = val

        if errors:
            for e in errors:
                flash(e, "error")
            return render_template("insert.html", prefill=raw, db_path=DB_PATH)

        if not record:
            flash("No fields provided.", "error")
            return render_template("insert.html", prefill=raw, db_path=DB_PATH)

        try:
            result = db.insert(record)
            flash(f"Inserted record {result['id'][:8]}…", "success")
            return redirect(url_for("index"))
        except (TypeError, ValueError) as e:
            flash(str(e), "error")
            return render_template("insert.html", prefill=raw, db_path=DB_PATH)

    return render_template("insert.html", prefill="", db_path=DB_PATH)


@app.route("/edit/<record_id>", methods=["GET", "POST"])
def edit(record_id):
    record = db.read(record_id=record_id)
    if not record:
        flash("Record not found.", "error")
        return redirect(url_for("index"))

    if request.method == "POST":
        raw = request.form.get("fields", "").strip()
        changes = {}
        errors  = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            if "=" not in line:
                errors.append(f"Invalid line: {line!r}")
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            if key in ("id", "created_at"):
                continue
            try:
                changes[key] = int(val)
            except ValueError:
                try:
                    changes[key] = float(val)
                except ValueError:
                    changes[key] = val

        if errors:
            for e in errors:
                flash(e, "error")
        elif not changes:
            flash("No changes provided.", "error")
        else:
            try:
                db.update(record_id, changes)
                flash("Record updated.", "success")
                return redirect(url_for("index"))
            except (TypeError, ValueError) as e:
                flash(str(e), "error")

    # Build prefill text from current record
    prefill_lines = [
        f"{k}={v}"
        for k, v in record.items()
        if k not in ("id", "created_at")
    ]
    prefill = "\n".join(prefill_lines)
    return render_template("edit.html", record=record, prefill=prefill, db_path=DB_PATH)


@app.route("/delete/<record_id>", methods=["POST"])
def delete(record_id):
    ok = db.delete(record_id)
    if ok:
        flash("Record deleted.", "success")
    else:
        flash("Record not found.", "error")
    return redirect(url_for("index"))


@app.route("/schema")
def schema():
    import sqlite3
    cur = db._conn.cursor()
    cur.execute("PRAGMA table_info(records)")
    columns = [dict(r) for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT field FROM field_index ORDER BY field")
    blob_fields = [r["field"] for r in cur.fetchall()]

    blob_stats = {}
    for field in blob_fields:
        cur.execute(
            "SELECT value, COUNT(*) cnt FROM field_index WHERE field=? GROUP BY value ORDER BY cnt DESC LIMIT 10",
            (field,),
        )
        blob_stats[field] = [dict(r) for r in cur.fetchall()]

    return render_template(
        "schema.html",
        columns=columns,
        blob_fields=blob_fields,
        blob_stats=blob_stats,
        db_path=DB_PATH,
    )


@app.route("/api/records")
def api_records():
    """Simple JSON API endpoint."""
    records = db.read()
    return jsonify({"count": len(records), "records": records})


if __name__ == "__main__":
    print(f"\n  FileDatabase GUI")
    print(f"  Database : {DB_PATH}")
    print(f"  Open     : http://127.0.0.1:5000\n")
    app.run(debug=True, port=5000)