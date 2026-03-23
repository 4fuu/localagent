"""Periodic scrub for sensitive values persisted in manifest DB."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .secrets import load_all_decrypted, scrub_text

_MANIFEST_DB = Path(".localagent/manifest.db")


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _list_tables(db: sqlite3.Connection) -> list[str]:
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _text_columns(db: sqlite3.Connection, table: str) -> list[str]:
    cols = db.execute(f"PRAGMA table_info({_qident(table)})").fetchall()
    result: list[str] = []
    for col in cols:
        # pragma columns: cid, name, type, notnull, dflt_value, pk
        col_name = str(col[1]) if len(col) > 1 else ""
        col_type = str(col[2]).upper() if len(col) > 2 and col[2] is not None else ""
        if not col_name:
            continue
        # SQLite type system is loose; we only scrub declared TEXT columns.
        if "TEXT" in col_type:
            result.append(col_name)
    return result


def scrub_manifest_db(db_path: str | Path | None = None) -> dict[str, Any]:
    """Scrub known secret values from TEXT fields in manifest DB.

    Returns summary stats for observability.
    """
    path = Path(db_path) if db_path else _MANIFEST_DB
    if not path.is_file():
        return {"ok": True, "updated_rows": 0, "updated_fields": 0, "tables": 0, "reason": "db_not_found"}

    secrets = load_all_decrypted()
    if not secrets:
        return {"ok": True, "updated_rows": 0, "updated_fields": 0, "tables": 0, "reason": "no_secrets"}

    db = sqlite3.connect(str(path), timeout=10.0)
    db.execute("PRAGMA busy_timeout=5000")
    updated_rows = 0
    updated_fields = 0
    table_count = 0
    try:
        for table in _list_tables(db):
            text_cols = _text_columns(db, table)
            if not text_cols:
                continue
            table_count += 1
            select_cols = ", ".join(_qident(c) for c in text_cols)
            rows = db.execute(f"SELECT rowid, {select_cols} FROM {_qident(table)}").fetchall()
            for row in rows:
                rowid = row[0]
                changed: dict[str, str] = {}
                for idx, col in enumerate(text_cols, start=1):
                    raw = row[idx]
                    if not isinstance(raw, str) or not raw:
                        continue
                    cleaned = scrub_text(raw, secrets)
                    if cleaned != raw:
                        changed[col] = cleaned
                if not changed:
                    continue
                set_sql = ", ".join(f"{_qident(k)} = ?" for k in changed.keys())
                params = list(changed.values()) + [rowid]
                db.execute(f"UPDATE {_qident(table)} SET {set_sql} WHERE rowid = ?", params)
                updated_rows += 1
                updated_fields += len(changed)
        db.commit()
        return {
            "ok": True,
            "updated_rows": updated_rows,
            "updated_fields": updated_fields,
            "tables": table_count,
        }
    finally:
        db.close()
