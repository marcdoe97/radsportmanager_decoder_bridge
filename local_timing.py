"""Local race timing store for the MYLAPS bridge and Streamlit dashboard."""

from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


DRIVER_ALIASES = {
    "start_number": ["start_number", "startnummer", "nummer", "nr", "bib"],
    "name": ["name", "fahrer", "fahrername", "teilnehmer"],
    "team": ["team", "verein", "club"],
    "short_id": ["short_id", "shortid", "transponder_short_id", "transponder", "chip", "chip_zuordnung"],
    "category": ["category", "kategorie", "klasse", "rennen"],
}

MAPPING_ALIASES = {
    "short_id": ["short_id", "shortid", "transponder_short_id", "short", "chip"],
    "long_id": ["long_id", "longid", "transponder_long_id", "transponder", "code", "chip_long_id"],
    "label": ["label", "bezeichnung", "name"],
}


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path) -> None:
    path = Path(db_path)
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True)

    with connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_number TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                team TEXT DEFAULT '',
                short_id TEXT NOT NULL UNIQUE,
                category TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS transponder_mapping (
                long_id TEXT PRIMARY KEY,
                short_id TEXT NOT NULL,
                label TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS passings (
                event_id TEXT PRIMARY KEY,
                chip_long_id TEXT NOT NULL,
                short_id TEXT NOT NULL,
                passing_time TEXT NOT NULL,
                metadata TEXT DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_passings_short_time
                ON passings (short_id, passing_time);
            CREATE INDEX IF NOT EXISTS idx_passings_time
                ON passings (passing_time);
            """
        )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_key(value: str) -> str:
    return (
        value.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "")
        .replace("ü", "ue")
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ß", "ss")
    )


def pick(row: dict, aliases: list[str], default: str = "") -> str:
    normalized = {normalize_key(str(key)): value for key, value in row.items()}
    for alias in aliases:
        value = normalized.get(normalize_key(alias))
        if value is not None:
            return str(value).strip()
    return default


def _clean_table_rows(rows: Iterable[dict]) -> list[dict]:
    clean_rows = []
    for row in rows:
        clean = {
            str(key).strip(): "" if value is None else str(value).strip()
            for key, value in row.items()
            if str(key).strip()
        }
        if any(clean.values()):
            clean_rows.append(clean)
    return clean_rows


def read_csv_rows(path: str | Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;	")
        return _clean_table_rows(csv.DictReader(handle, dialect=dialect))


def import_drivers(db_path: str | Path, rows: Iterable[dict], replace: bool = True) -> int:
    init_db(db_path)
    timestamp = now_iso()
    imported = 0
    with connect(db_path) as conn:
        if replace:
            conn.execute("DELETE FROM drivers")
        for row in rows:
            start_number = pick(row, DRIVER_ALIASES["start_number"])
            name = pick(row, DRIVER_ALIASES["name"])
            short_id = pick(row, DRIVER_ALIASES["short_id"])
            if not start_number or not name or not short_id:
                continue
            conn.execute(
                """
                INSERT INTO drivers (start_number, name, team, short_id, category, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(start_number) DO UPDATE SET
                    name = excluded.name,
                    team = excluded.team,
                    short_id = excluded.short_id,
                    category = excluded.category,
                    updated_at = excluded.updated_at
                """,
                (
                    start_number,
                    name,
                    pick(row, DRIVER_ALIASES["team"]),
                    short_id,
                    pick(row, DRIVER_ALIASES["category"]),
                    timestamp,
                    timestamp,
                ),
            )
            imported += 1
    return imported


def import_mapping(db_path: str | Path, rows: Iterable[dict], replace: bool = True) -> int:
    init_db(db_path)
    timestamp = now_iso()
    imported = 0
    with connect(db_path) as conn:
        if replace:
            conn.execute("DELETE FROM transponder_mapping")
        for row in rows:
            long_id = pick(row, MAPPING_ALIASES["long_id"])
            short_id = pick(row, MAPPING_ALIASES["short_id"])
            if not long_id or not short_id:
                continue
            conn.execute(
                """
                INSERT INTO transponder_mapping (long_id, short_id, label, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(long_id) DO UPDATE SET
                    short_id = excluded.short_id,
                    label = excluded.label,
                    updated_at = excluded.updated_at
                """,
                (
                    long_id,
                    short_id,
                    pick(row, MAPPING_ALIASES["label"]),
                    timestamp,
                    timestamp,
                ),
            )
            imported += 1
    return imported


def load_mapping_dict(db_path: str | Path) -> dict[str, str]:
    init_db(db_path)
    with connect(db_path) as conn:
        rows = conn.execute("SELECT long_id, short_id FROM transponder_mapping").fetchall()
    mapping = {}
    for row in rows:
        long_id = row["long_id"]
        short_id = row["short_id"]
        mapping[long_id] = short_id
        mapping[long_id.upper()] = short_id
        mapping[long_id.lower()] = short_id
    return mapping


def resolve_short_id(db_path: str | Path, long_id: str) -> str | None:
    init_db(db_path)
    candidates = (long_id, long_id.upper(), long_id.lower())
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT short_id FROM transponder_mapping
            WHERE long_id IN (?, ?, ?)
            LIMIT 1
            """,
            candidates,
        ).fetchone()
    return row["short_id"] if row else None


def record_passing(db_path: str | Path, payload: dict) -> None:
    init_db(db_path)
    timestamp = now_iso()
    metadata = {
        key: value
        for key, value in payload.items()
        if key not in {"event_id", "chip_long_id", "short_id", "passing_time"}
    }
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO passings (
                event_id, chip_long_id, short_id, passing_time, metadata, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["event_id"],
                payload["chip_long_id"],
                payload["short_id"],
                payload["passing_time"],
                json.dumps(metadata, ensure_ascii=False),
                timestamp,
            ),
        )
