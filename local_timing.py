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


class ClosingConnection(sqlite3.Connection):
    """SQLite context manager that also releases the file handle on exit."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, factory=ClosingConnection)
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
                created_at TEXT NOT NULL,
                updated_at TEXT,
                decoder_id TEXT,
                passing_number INTEGER,
                delivery_status TEXT NOT NULL DEFAULT 'PENDING',
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS bridge_status (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                registry_last_success TEXT,
                registry_last_error TEXT,
                registry_entries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS decoder_sequence (
                decoder_id TEXT PRIMARY KEY,
                last_passing_number INTEGER NOT NULL,
                last_passing_time TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS decoder_gaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decoder_id TEXT NOT NULL,
                previous_passing_number INTEGER NOT NULL,
                current_passing_number INTEGER NOT NULL,
                missing_count INTEGER NOT NULL,
                previous_passing_time TEXT,
                current_passing_time TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                UNIQUE(decoder_id, previous_passing_number, current_passing_number)
            );

            CREATE INDEX IF NOT EXISTS idx_passings_short_time
                ON passings (short_id, passing_time);
            CREATE INDEX IF NOT EXISTS idx_passings_time
                ON passings (passing_time);
            CREATE INDEX IF NOT EXISTS idx_decoder_gaps_detected
                ON decoder_gaps (detected_at);
            """
        )
        existing_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(passings)").fetchall()
        }
        additions = {
            "updated_at": "TEXT",
            "decoder_id": "TEXT",
            "passing_number": "INTEGER",
            # Rows from versions without delivery tracking must not be sent
            # again blindly; their historic server state is unknown.
            "delivery_status": "TEXT NOT NULL DEFAULT 'UNKNOWN'",
            "delivery_attempts": "INTEGER NOT NULL DEFAULT 0",
            "last_error": "TEXT",
        }
        for column, definition in additions.items():
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE passings ADD COLUMN {column} {definition}")
        conn.execute("UPDATE passings SET updated_at=COALESCE(updated_at,created_at)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_passings_delivery ON passings (delivery_status, passing_time)"
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


def _record_decoder_sequence(conn: sqlite3.Connection, payload: dict, timestamp: str) -> dict | None:
    decoder_id = str(payload.get("decoder_id") or "").strip()
    raw_number = payload.get("passing_number")
    if not decoder_id or raw_number is None:
        return None
    try:
        current = int(raw_number)
    except (TypeError, ValueError):
        return None
    passing_time = str(payload.get("passing_time") or timestamp)
    previous = conn.execute(
        "SELECT last_passing_number,last_passing_time FROM decoder_sequence WHERE decoder_id=?",
        (decoder_id,),
    ).fetchone()
    gap = None
    if previous and current > int(previous["last_passing_number"]) + 1:
        previous_number = int(previous["last_passing_number"])
        missing_count = current - previous_number - 1
        conn.execute(
            """INSERT OR IGNORE INTO decoder_gaps (
                   decoder_id,previous_passing_number,current_passing_number,missing_count,
                   previous_passing_time,current_passing_time,detected_at
               ) VALUES (?,?,?,?,?,?,?)""",
            (
                decoder_id,
                previous_number,
                current,
                missing_count,
                str(previous["last_passing_time"] or ""),
                passing_time,
                timestamp,
            ),
        )
        gap = {
            "decoder_id": decoder_id,
            "previous_passing_number": previous_number,
            "current_passing_number": current,
            "missing_count": missing_count,
        }

    should_advance = previous is None or current > int(previous["last_passing_number"])
    # A lower sequence with a later decoder timestamp indicates a decoder
    # restart. It becomes the new baseline instead of producing endless gaps.
    if previous and current < int(previous["last_passing_number"]):
        should_advance = passing_time > str(previous["last_passing_time"] or "")
    if should_advance:
        conn.execute(
            """INSERT INTO decoder_sequence (decoder_id,last_passing_number,last_passing_time,updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(decoder_id) DO UPDATE SET
                   last_passing_number=excluded.last_passing_number,
                   last_passing_time=excluded.last_passing_time,
                   updated_at=excluded.updated_at""",
            (decoder_id, current, passing_time, timestamp),
        )
    return gap


def record_passing(db_path: str | Path, payload: dict) -> dict | None:
    init_db(db_path)
    timestamp = now_iso()
    metadata = {
        key: value
        for key, value in payload.items()
        if key not in {"event_id", "chip_long_id", "short_id", "passing_time"}
    }
    gap = None
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO passings (
                event_id, chip_long_id, short_id, passing_time, metadata, status,
                created_at, updated_at, decoder_id, passing_number, delivery_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                short_id=CASE WHEN excluded.short_id<>'' THEN excluded.short_id ELSE passings.short_id END,
                metadata=excluded.metadata,
                status=CASE
                    WHEN passings.status='IGNORED' THEN 'IGNORED'
                    WHEN excluded.short_id<>'' THEN 'ACTIVE'
                    ELSE passings.status
                END,
                updated_at=excluded.updated_at,
                decoder_id=COALESCE(NULLIF(excluded.decoder_id,''),passings.decoder_id),
                passing_number=COALESCE(excluded.passing_number,passings.passing_number)
            """,
            (
                payload["event_id"],
                payload["chip_long_id"],
                str(payload.get("short_id") or ""),
                payload["passing_time"],
                json.dumps(metadata, ensure_ascii=False),
                "ACTIVE" if str(payload.get("short_id") or "").strip() else "UNMAPPED",
                timestamp,
                timestamp,
                str(payload.get("decoder_id") or ""),
                payload.get("passing_number"),
                "PENDING",
            ),
        )
        gap = _record_decoder_sequence(conn, payload, timestamp)
    return gap


def update_passing_delivery(
    db_path: str | Path,
    event_id: str,
    delivery_status: str,
    error: str = "",
    short_id: str = "",
) -> None:
    init_db(db_path)
    timestamp = now_iso()
    with connect(db_path) as conn:
        conn.execute(
            """UPDATE passings
               SET delivery_status=?,
                   delivery_attempts=delivery_attempts+CASE WHEN ? IN ('RETRY','FAILED') THEN 1 ELSE 0 END,
                   last_error=NULLIF(?,''),
                   short_id=CASE WHEN ?<>'' THEN ? ELSE short_id END,
                   status=CASE
                       WHEN status='IGNORED' THEN status
                       WHEN ?<>'' THEN 'ACTIVE'
                       WHEN ?='UNMAPPED' THEN 'UNMAPPED'
                       ELSE status
                   END,
                   updated_at=?
               WHERE event_id=?""",
            (
                delivery_status,
                delivery_status,
                error,
                short_id,
                short_id,
                short_id,
                delivery_status,
                timestamp,
                event_id,
            ),
        )


def update_bridge_status(
    db_path: str | Path,
    registry_last_success: str | None,
    registry_last_error: str | None,
    registry_entries: int,
) -> None:
    init_db(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """INSERT INTO bridge_status (
                   id, registry_last_success, registry_last_error, registry_entries, updated_at
               ) VALUES (1, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   registry_last_success=excluded.registry_last_success,
                   registry_last_error=excluded.registry_last_error,
                   registry_entries=excluded.registry_entries,
                   updated_at=excluded.updated_at""",
            (registry_last_success, registry_last_error, int(registry_entries), now_iso()),
        )


def pending_delivery_payloads(db_path: str | Path, limit: int = 10000) -> list[dict]:
    """Return locally recorded passings that still need durable delivery."""
    init_db(db_path)
    safe_limit = max(1, min(int(limit), 100_000))
    with connect(db_path) as conn:
        rows = conn.execute(
            """SELECT event_id, chip_long_id, short_id, passing_time, metadata
               FROM passings
               WHERE delivery_status IN ('PENDING','RETRY','UNMAPPED')
               ORDER BY passing_time,event_id
               LIMIT ?""",
            (safe_limit,),
        ).fetchall()
    payloads: list[dict] = []
    for row in rows:
        try:
            metadata = json.loads(row["metadata"] or "{}")
        except (TypeError, ValueError):
            metadata = {}
        payload = {
            "event_id": str(row["event_id"]),
            "chip_long_id": str(row["chip_long_id"] or ""),
            "short_id": str(row["short_id"] or ""),
            "passing_time": str(row["passing_time"]),
        }
        if isinstance(metadata, dict):
            payload.update(metadata)
        payloads.append(payload)
    return payloads
