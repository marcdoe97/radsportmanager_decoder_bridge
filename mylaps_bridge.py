#!/usr/bin/env python3
"""
MYLAPS ProChip Bridge
Verbindet sich mit dem ProChip Smart Decoder via AMB P3 oder DCI-Protokoll (TCP)
und leitet Transponder-Passings an den Radsportmanager-Server weiter.
"""

import configparser
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import random
import signal
import socket
import sqlite3
import struct
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import requests

try:
    import local_timing
except ImportError:
    local_timing = None

# ---------------------------------------------------------------------------
# Konfiguration laden
# ---------------------------------------------------------------------------

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.ini")

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8-sig")

DECODER_HOST = config.get("decoder", "host", fallback="192.168.1.100")
DECODER_PORT = config.getint("decoder", "port", fallback=5403)
DECODER_PROTOCOL = config.get("decoder", "protocol", fallback="amb_p3").lower()
DECODER_STARTUP_HEX = config.get("decoder", "startup_hex", fallback="").strip()
RECONNECT_INTERVAL = config.getint("decoder", "reconnect_interval", fallback=5)

SERVER_ENABLED = config.getboolean("server", "enabled", fallback=True)
API_URL = config.get("server", "api_url", fallback="")
BATCH_API_URL = config.get("server", "batch_api_url", fallback="")
REGISTRY_URL = config.get("server", "registry_url", fallback="")
API_KEY = config.get("server", "api_key", fallback="")
HTTP_TIMEOUT = config.getint("server", "timeout", fallback=5)

SIMULATE = config.getboolean("bridge", "simulate", fallback=False)
REGISTRY_REFRESH = config.getint("bridge", "registry_refresh_interval", fallback=60)
BUFFER_DB = config.get("bridge", "buffer_db", fallback="buffer.db")
LOG_LEVEL = config.get("bridge", "log_level", fallback="INFO")
QUEUE_MAX_SIZE = config.getint("bridge", "queue_max_size", fallback=5000)
BATCH_SIZE = config.getint("bridge", "batch_size", fallback=100)
BATCH_FLUSH_INTERVAL = config.getfloat("bridge", "batch_flush_interval", fallback=0.5)
HTTP_WORKERS = config.getint("bridge", "http_workers", fallback=1)
LOG_FILE = config.get("bridge", "log_file", fallback="bridge.log")
LOG_MAX_BYTES = config.getint("bridge", "log_max_bytes", fallback=5_000_000)
LOG_BACKUP_COUNT = config.getint("bridge", "log_backup_count", fallback=5)
SIM_AVERAGE_SPEED_KMH = config.getfloat("bridge", "simulation_average_speed_kmh", fallback=45.0)
SIM_SPEED_FACTOR = config.getfloat("bridge", "simulation_speed_factor", fallback=10.0)
SIM_LAP_LENGTH_KM = config.getfloat("bridge", "simulation_lap_length_km", fallback=1.0)
LOCAL_TIMING_ENABLED = config.getboolean("local", "enabled", fallback=True)
LOCAL_TIMING_DB = config.get("local", "db", fallback="local_timing.db")
LOCAL_TIMEZONE = ZoneInfo("Europe/Berlin")

if not os.path.isabs(BUFFER_DB):
    BUFFER_DB = os.path.join(os.path.dirname(__file__), BUFFER_DB)
if not os.path.isabs(LOCAL_TIMING_DB):
    LOCAL_TIMING_DB = os.path.join(os.path.dirname(__file__), LOCAL_TIMING_DB)
if LOG_FILE and not os.path.isabs(LOG_FILE):
    LOG_FILE = os.path.join(os.path.dirname(__file__), LOG_FILE)

if not BATCH_API_URL and API_URL:
    parts = urlsplit(API_URL)
    BATCH_API_URL = urlunsplit(
        (parts.scheme, parts.netloc, parts.path.replace("passing.php", "passing_batch.php"), parts.query, parts.fragment)
    )

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log_handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
if LOG_FILE:
    log_handlers.append(
        RotatingFileHandler(
            LOG_FILE,
            maxBytes=max(100_000, LOG_MAX_BYTES),
            backupCount=max(1, LOG_BACKUP_COUNT),
            encoding="utf-8",
        )
    )
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=log_handlers,
)
log = logging.getLogger("mylaps_bridge")

# ---------------------------------------------------------------------------
# Offline-Puffer (SQLite)
# ---------------------------------------------------------------------------

def init_buffer(db_path: str) -> sqlite3.Connection:
    db_dir = os.path.dirname(os.path.abspath(db_path))
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS buffer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            event_id TEXT,
            chip_long_id TEXT,
            short_id TEXT,
            state TEXT NOT NULL DEFAULT 'PENDING',
            next_attempt_at REAL NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at TEXT,
            decoder_id TEXT,
            passing_number INTEGER
        )"""
    )
    existing_columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(buffer)").fetchall()
    }
    additions = {
        "event_id": "TEXT",
        "chip_long_id": "TEXT",
        "short_id": "TEXT",
        "state": "TEXT NOT NULL DEFAULT 'PENDING'",
        "next_attempt_at": "REAL NOT NULL DEFAULT 0",
        "last_error": "TEXT",
        "updated_at": "TEXT",
        "decoder_id": "TEXT",
        "passing_number": "INTEGER",
    }
    for column, definition in additions.items():
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE buffer ADD COLUMN {column} {definition}")

    # Alte Pufferdaten werden ohne Verlust in das neue Zustandsmodell uebernommen.
    for row in conn.execute(
        "SELECT id, payload FROM buffer WHERE event_id IS NULL OR event_id=''"
    ).fetchall():
        try:
            payload = json.loads(row["payload"])
        except (TypeError, ValueError):
            payload = {}
        event_id = str(payload.get("event_id") or f"legacy-buffer-{row['id']}")
        conn.execute(
            """UPDATE buffer
               SET event_id=?, chip_long_id=?, short_id=?, decoder_id=?, passing_number=?,
                   state=COALESCE(NULLIF(state,''),'PENDING'), updated_at=COALESCE(updated_at,created_at)
               WHERE id=?""",
            (
                event_id,
                str(payload.get("chip_long_id") or ""),
                str(payload.get("short_id") or ""),
                str(payload.get("decoder_id") or ""),
                payload.get("passing_number"),
                int(row["id"]),
            ),
        )
    conn.execute(
        "DELETE FROM buffer WHERE id NOT IN (SELECT MIN(id) FROM buffer GROUP BY event_id)"
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uniq_buffer_event ON buffer(event_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_buffer_due ON buffer(state,next_attempt_at,id)"
    )
    conn.commit()
    return conn


def buffer_put(conn: sqlite3.Connection, payload: dict, state: str = "PENDING"):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO buffer (
               payload, created_at, attempts, event_id, chip_long_id, short_id,
               state, next_attempt_at, last_error, updated_at, decoder_id, passing_number
           ) VALUES (?, ?, 0, ?, ?, ?, ?, 0, NULL, ?, ?, ?)
           ON CONFLICT(event_id) DO UPDATE SET
               payload=excluded.payload,
               chip_long_id=excluded.chip_long_id,
               short_id=CASE WHEN excluded.short_id<>'' THEN excluded.short_id ELSE buffer.short_id END,
               state=CASE WHEN buffer.state='FAILED' THEN buffer.state ELSE excluded.state END,
               next_attempt_at=CASE WHEN buffer.state='FAILED' THEN buffer.next_attempt_at ELSE 0 END,
               updated_at=excluded.updated_at,
               decoder_id=COALESCE(NULLIF(excluded.decoder_id,''),buffer.decoder_id),
               passing_number=COALESCE(excluded.passing_number,buffer.passing_number)""",
        (
            json.dumps(payload, ensure_ascii=False),
            now,
            str(payload.get("event_id") or ""),
            str(payload.get("chip_long_id") or ""),
            str(payload.get("short_id") or ""),
            state,
            now,
            str(payload.get("decoder_id") or ""),
            payload.get("passing_number"),
        ),
    )
    conn.commit()


def _retry_delay(attempts: int) -> float:
    return min(300.0, max(2.0, float(2 ** min(max(attempts, 1), 8))))


def _mark_local_delivery(event_id: str, delivery_status: str, error: str = "", short_id: str = ""):
    if not (LOCAL_TIMING_ENABLED and local_timing):
        return
    try:
        local_timing.update_passing_delivery(
            LOCAL_TIMING_DB,
            event_id,
            delivery_status,
            error=error,
            short_id=short_id,
        )
    except Exception as exc:
        log.warning("Lokaler Passing-Status konnte nicht aktualisiert werden: %s", exc)


def buffer_flush(conn: sqlite3.Connection) -> int:
    """Sendet faellige Passings geordnet und wertet jeden Serverstatus einzeln aus."""
    now_epoch = time.time()
    with buffer_lock:
        rows = conn.execute(
            """SELECT id, payload, attempts
               FROM buffer
               WHERE state IN ('PENDING','UNMAPPED','RETRY') AND next_attempt_at<=?
               ORDER BY id
               LIMIT ?""",
            (now_epoch, BATCH_SIZE),
        ).fetchall()
    if not rows:
        return 0

    send_rows: list[tuple[int, int, dict]] = []
    for row in rows:
        row_id = int(row["id"])
        try:
            payload = json.loads(row["payload"])
        except (TypeError, ValueError):
            with buffer_lock:
                conn.execute(
                    "UPDATE buffer SET state='FAILED',last_error='Ungueltiges lokales JSON',updated_at=? WHERE id=?",
                    (datetime.now(timezone.utc).isoformat(), row_id),
                )
                conn.commit()
            continue

        short_id = str(payload.get("short_id") or "").strip()
        if not short_id:
            short_id = resolve_long_id(str(payload.get("chip_long_id") or "")) or ""
            if short_id:
                payload["short_id"] = short_id
                with buffer_lock:
                    conn.execute(
                        "UPDATE buffer SET payload=?,short_id=?,state='PENDING',next_attempt_at=0,last_error=NULL,updated_at=? WHERE id=?",
                        (json.dumps(payload, ensure_ascii=False), short_id, datetime.now(timezone.utc).isoformat(), row_id),
                    )
                    conn.commit()
                _mark_local_delivery(str(payload.get("event_id") or ""), "PENDING", short_id=short_id)
            else:
                message = "Long-ID noch nicht in der Registry"
                with buffer_lock:
                    conn.execute(
                        "UPDATE buffer SET state='UNMAPPED',next_attempt_at=?,last_error=?,updated_at=? WHERE id=?",
                        (
                            now_epoch + max(5, REGISTRY_REFRESH),
                            message,
                            datetime.now(timezone.utc).isoformat(),
                            row_id,
                        ),
                    )
                    conn.commit()
                _mark_local_delivery(str(payload.get("event_id") or ""), "UNMAPPED", message)
                continue

        send_rows.append((row_id, int(row["attempts"] or 0), payload))

    if not send_rows:
        return len(rows)

    outcomes = send_batch_to_server([payload for _, _, payload in send_rows])
    updated_at = datetime.now(timezone.utc).isoformat()
    with buffer_lock:
        for row_id, attempts, payload in send_rows:
            event_id = str(payload.get("event_id") or "")
            outcome = outcomes.get(event_id, {"disposition": "RETRY", "reason": "Keine Einzelbestaetigung vom Server"})
            disposition = str(outcome.get("disposition") or "RETRY").upper()
            reason = str(outcome.get("reason") or "")[:1000]
            if disposition in {"ACK", "DUPLICATE"}:
                conn.execute("DELETE FROM buffer WHERE id=?", (row_id,))
                _mark_local_delivery(event_id, "ACKED", reason, str(payload.get("short_id") or ""))
            elif disposition == "REJECTED":
                conn.execute(
                    "UPDATE buffer SET state='FAILED',attempts=attempts+1,last_error=?,updated_at=? WHERE id=?",
                    (reason or "Server hat Passing dauerhaft abgelehnt", updated_at, row_id),
                )
                _mark_local_delivery(event_id, "FAILED", reason, str(payload.get("short_id") or ""))
            else:
                new_attempts = attempts + 1
                conn.execute(
                    "UPDATE buffer SET state='RETRY',attempts=?,next_attempt_at=?,last_error=?,updated_at=? WHERE id=?",
                    (new_attempts, now_epoch + _retry_delay(new_attempts), reason, updated_at, row_id),
                )
                _mark_local_delivery(event_id, "RETRY", reason, str(payload.get("short_id") or ""))
        conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Transponder-Registry (Long ID -> Short ID)
# ---------------------------------------------------------------------------

registry: dict[str, str] = {}
registry_lock = threading.Lock()
registry_last_success: str | None = None
registry_last_error: str | None = None


def load_registry():
    global registry, registry_last_success, registry_last_error
    loaded_remote = False
    if SERVER_ENABLED and REGISTRY_URL:
        try:
            resp = requests.get(
                REGISTRY_URL,
                headers={"X-API-Key": API_KEY},
                timeout=HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            remote_registry = {}
            for entry in data:
                long_id = str(entry.get("long_id") or "").strip()
                short_id = str(entry.get("short_id") or "").strip()
                if not long_id or not short_id:
                    continue
                remote_registry[long_id] = short_id
                remote_registry[long_id.upper()] = short_id
                remote_registry[long_id.lower()] = short_id
            with registry_lock:
                registry = remote_registry
            loaded_remote = True
            registry_last_success = datetime.now(timezone.utc).isoformat()
            registry_last_error = None
            log.info("Transponder-Registry geladen: %d Einträge", len(registry))
        except Exception as e:
            registry_last_error = str(e)
            log.warning("Registry konnte nicht geladen werden: %s", e)

    if LOCAL_TIMING_ENABLED and local_timing:
        try:
            local_registry = local_timing.load_mapping_dict(LOCAL_TIMING_DB)
            if local_registry:
                with registry_lock:
                    registry.update(local_registry)
                log.info("Lokale Transponder-Registry geladen: %d Einträge", len(local_registry))
        except Exception as e:
            log.warning("Lokale Registry konnte nicht geladen werden: %s", e)

    if LOCAL_TIMING_ENABLED and local_timing:
        try:
            local_timing.update_bridge_status(
                LOCAL_TIMING_DB,
                registry_last_success=registry_last_success,
                registry_last_error=registry_last_error,
                registry_entries=len(registry),
            )
        except Exception as e:
            log.debug("Registry-Status konnte lokal nicht gespeichert werden: %s", e)
    if loaded_remote or registry:
        sender_wakeup.set()


def registry_worker():
    while not shutdown_event.is_set():
        if shutdown_event.wait(REGISTRY_REFRESH):
            break
        load_registry()


def resolve_long_id(long_id: str) -> str | None:
    candidates = [long_id, long_id.upper(), long_id.lower()]
    with registry_lock:
        for candidate in candidates:
            if candidate in registry:
                return registry[candidate]
    if LOCAL_TIMING_ENABLED and local_timing:
        try:
            return local_timing.resolve_short_id(LOCAL_TIMING_DB, long_id)
        except Exception as e:
            log.warning("Lokale Short-ID-Aufloesung fehlgeschlagen: %s", e)
    return None


# ---------------------------------------------------------------------------
# HTTP: Passing an Server senden
# ---------------------------------------------------------------------------

buffer_conn: sqlite3.Connection | None = None
buffer_lock = threading.Lock()
sender_wakeup = threading.Event()


def _retry_outcomes(payloads: list[dict], reason: str) -> dict[str, dict]:
    return {
        str(payload.get("event_id") or ""): {"disposition": "RETRY", "reason": reason}
        for payload in payloads
    }


def parse_server_outcomes(payloads: list[dict], data: dict) -> dict[str, dict]:
    """Normalisiert neue und alte API-Antworten ohne Passings still zu verlieren."""
    outcomes: dict[str, dict] = {}
    results = data.get("results")
    if isinstance(results, list):
        for result in results:
            if not isinstance(result, dict):
                continue
            event_id = str(result.get("event_id") or "")
            if not event_id:
                continue
            disposition = str(result.get("disposition") or "").upper()
            if disposition not in {"ACK", "DUPLICATE", "RETRY", "REJECTED"}:
                disposition = "RETRY"
            outcomes[event_id] = {
                "disposition": disposition,
                "reason": str(result.get("reason") or result.get("message") or ""),
            }

    # Rueckwaertskompatibilitaet: Eine alte API bestaetigt nur den ganzen Batch.
    if not outcomes:
        successful = bool(data.get("success"))
        retryable = (
            int(data.get("unresolved") or 0) > 0
            or int(data.get("errors") or 0) > 0
            or int(data.get("ignored") or 0) > 0
        )
        disposition = "ACK" if successful and not retryable else "RETRY"
        reason = "" if disposition == "ACK" else "Alte API ohne eindeutige Einzelbestaetigung"
        return {
            str(payload.get("event_id") or ""): {"disposition": disposition, "reason": reason}
            for payload in payloads
        }

    for payload in payloads:
        event_id = str(payload.get("event_id") or "")
        outcomes.setdefault(
            event_id,
            {"disposition": "RETRY", "reason": "Serverantwort enthaelt keinen Status fuer dieses Passing"},
        )
    return outcomes


def send_to_server(payload: dict, use_buffer: bool = True) -> dict:
    """Kompatibilitaets-Wrapper; die produktive Zustellung verwendet den Batch-Endpunkt."""
    return send_batch_to_server([payload]).get(
        str(payload.get("event_id") or ""),
        {"disposition": "RETRY", "reason": "Keine Serverbestaetigung"},
    )


def send_batch_to_server(payloads: list[dict]) -> dict[str, dict]:
    if not payloads:
        return {}
    if not SERVER_ENABLED:
        return {
            str(payload.get("event_id") or ""): {"disposition": "ACK", "reason": "Server deaktiviert"}
            for payload in payloads
        }
    try:
        resp = requests.post(
            BATCH_API_URL,
            json={"passings": payloads},
            headers={"X-API-Key": API_KEY},
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            log.warning("Batch-Endpoint antwortete %s: %s", resp.status_code, resp.text[:200])
            return _retry_outcomes(payloads, f"HTTP {resp.status_code}")

        try:
            data = resp.json()
        except ValueError:
            data = {}

        if not data.get("success", False):
            log.warning("Batch wurde vom Server abgelehnt: %s", data)
            return _retry_outcomes(payloads, str(data.get("message") or "Batch abgelehnt"))

        log.info(
            "Batch gesendet: received=%s accepted=%s ignored=%s unresolved=%s duplicate=%s",
            data.get("received", len(payloads)),
            data.get("accepted", 0),
            data.get("ignored", 0),
            data.get("unresolved", 0),
            data.get("duplicate", 0),
        )
        return parse_server_outcomes(payloads, data)
    except Exception as e:
        log.warning("Netzwerkfehler beim Batch-Senden: %s", e)
        return _retry_outcomes(payloads, str(e))


def enqueue_payload(payload: dict, wake_sender: bool = True) -> bool:
    if not buffer_conn:
        log.error("Dauerhafter Puffer nicht bereit: %s", payload.get("event_id", ""))
        return False
    state = "PENDING" if str(payload.get("short_id") or "").strip() else "UNMAPPED"
    with buffer_lock:
        buffer_put(buffer_conn, payload, state=state)
    if wake_sender:
        sender_wakeup.set()
    return True


def recover_local_pending() -> int:
    """Rebuild the send buffer after a crash between local storage and enqueue."""
    if not (SERVER_ENABLED and LOCAL_TIMING_ENABLED and local_timing and buffer_conn):
        return 0
    try:
        payloads = local_timing.pending_delivery_payloads(LOCAL_TIMING_DB)
    except Exception as exc:
        log.warning("Lokale offene Zustellungen konnten nicht geladen werden: %s", exc)
        return 0
    with buffer_lock:
        for payload in payloads:
            state = "PENDING" if str(payload.get("short_id") or "").strip() else "UNMAPPED"
            buffer_put(buffer_conn, payload, state=state)
    if payloads:
        log.info("%d lokal offene Passings in den Sendepuffer uebernommen", len(payloads))
        sender_wakeup.set()
    return len(payloads)


def http_sender_worker(worker_id: int):
    log.info("Geordneter HTTP-Sender %s gestartet", worker_id)
    while not shutdown_event.is_set():
        processed = buffer_flush(buffer_conn) if buffer_conn else 0
        if processed >= BATCH_SIZE:
            continue
        sender_wakeup.wait(max(0.1, BATCH_FLUSH_INTERVAL))
        sender_wakeup.clear()


def build_payload(
    long_id: str,
    short_id: str,
    passing_time_iso: str,
    metadata: dict | None = None,
) -> dict:
    metadata = metadata or {}
    decoder_id = metadata.get("decoder_id")
    passing_number = metadata.get("passing_number")
    if decoder_id is not None and passing_number is not None:
        # Passing numbers may restart after a decoder reboot, therefore the
        # decoder timestamp is part of the durable idempotency key as well.
        identity = f"decoder:{decoder_id}:passing:{passing_number}:time:{passing_time_iso}"
    else:
        identity = f"chip:{long_id}:time:{passing_time_iso}"
    event_id = hashlib.sha256(identity.encode()).hexdigest()[:32]
    payload = {
        "event_id": event_id,
        "chip_long_id": long_id,
        "short_id": short_id,
        "passing_time": passing_time_iso,
    }
    payload.update({k: v for k, v in metadata.items() if v is not None})
    return payload


def handle_passing(long_id: str, passing_dt: datetime, metadata: dict | None = None):
    passing_time_iso = passing_dt.astimezone(timezone.utc).isoformat()
    short_id = resolve_long_id(long_id) or ""
    payload = build_payload(long_id, short_id, passing_time_iso, metadata)
    queued = False
    if SERVER_ENABLED:
        # The send buffer is the first durable write. A process crash can then
        # never leave a decoder passing only in memory.
        queued = enqueue_payload(payload, wake_sender=False)
    if LOCAL_TIMING_ENABLED and local_timing:
        try:
            sequence_gap = local_timing.record_passing(LOCAL_TIMING_DB, payload)
            if sequence_gap:
                log.error(
                    "Decoder-Sequenzluecke: Decoder %s sprang von Passing %s auf %s (%s fehlen)",
                    sequence_gap["decoder_id"],
                    sequence_gap["previous_passing_number"],
                    sequence_gap["current_passing_number"],
                    sequence_gap["missing_count"],
                )
        except Exception as e:
            log.warning("Lokales Speichern des Passings fehlgeschlagen: %s", e)

    if short_id:
        log.info("LongID %s -> ShortID %s", long_id, short_id)
    else:
        log.warning(
            "Unbekannte Long ID dauerhaft vorgemerkt: %s (event_id=%s)",
            long_id,
            payload["event_id"],
        )

    if queued:
        sender_wakeup.set()
    elif not SERVER_ENABLED:
        _mark_local_delivery(payload["event_id"], "LOCAL_ONLY", short_id=short_id)


# ---------------------------------------------------------------------------
# AMB/MyLaps P3 Parser
#
# Smart Decoder Mitschnitte zeigen AMB P3 Records:
#
#   [0x8E] [VERSION 1B] [LENGTH 2B little-endian] [CRC 2B]
#   [FLAGS 2B] [TOR 2B little-endian] [FIELDS ...] [0x8F]
#
# TOR 0x0001 = PASSING. Felder sind Type/Length/Value-Tuples.
# Fuer den Radsportmanager ist vor allem TRAN_CODE (z.B. CT-61033)
# plus RTC/UTC-Zeit relevant. Strength/Hits/Decoder-ID gehen optional mit.
# ---------------------------------------------------------------------------

AMB_SOR = 0x8E
AMB_EOR = 0x8F
AMB_ESC = 0x8D
AMB_ESC_SUB = 0x20

AMB_TOR_PASSING = 0x0001

AMB_F_PASSING_NUMBER = 0x01
AMB_F_TRANSPONDER = 0x03
AMB_F_RTC_TIME = 0x04
AMB_F_STRENGTH = 0x05
AMB_F_HITS = 0x06
AMB_F_TRAN_CODE = 0x0A
AMB_F_UTC_TIME = 0x10
AMB_F_DECODER_ID = 0x81


def _u16le(data: bytes) -> int:
    return struct.unpack("<H", data)[0]


def _u32le(data: bytes) -> int:
    return struct.unpack("<I", data)[0]


def _u64le(data: bytes) -> int:
    return struct.unpack("<Q", data)[0]


def unescape_amb_p3(frame: bytes) -> bytes:
    if len(frame) <= 2:
        return frame

    out = bytearray([frame[0]])
    escaped = False
    for byte in frame[1:-1]:
        if escaped:
            out.append((byte - AMB_ESC_SUB) & 0xFF)
            escaped = False
        elif byte == AMB_ESC:
            escaped = True
        else:
            out.append(byte)
    out.append(frame[-1])
    return bytes(out)


def decode_transponder_code(raw: bytes) -> str | None:
    code = raw.rstrip(b"\x00 ").decode("ascii", errors="ignore").strip()
    return code or None


def micros_to_datetime(micros: int) -> datetime:
    return datetime.fromtimestamp(micros / 1_000_000.0, tz=timezone.utc)


def rtc_micros_to_datetime(micros: int) -> datetime:
    utc_like = datetime.fromtimestamp(micros / 1_000_000.0, tz=timezone.utc)
    local_wall_time = utc_like.replace(tzinfo=LOCAL_TIMEZONE)
    return local_wall_time.astimezone(timezone.utc)


def parse_amb_p3_record(frame: bytes):
    frame = unescape_amb_p3(frame)
    if len(frame) < 11 or frame[0] != AMB_SOR or frame[-1] != AMB_EOR:
        return

    record_len = _u16le(frame[2:4])
    if record_len != len(frame):
        log.debug("AMB P3 Laenge abweichend: Header=%s Ist=%s", record_len, len(frame))

    tor = _u16le(frame[8:10])
    if tor != AMB_TOR_PASSING:
        log.debug("AMB P3 Record ignoriert: TOR=0x%04X", tor)
        return

    body_end = len(frame) - 1
    fields: dict[int, bytes] = {}
    offset = 10
    while offset + 2 <= body_end:
        field_type = frame[offset]
        field_len = frame[offset + 1]
        value_start = offset + 2
        value_end = value_start + field_len
        if value_end > body_end:
            log.debug("AMB P3 Feld abgeschnitten: type=0x%02X len=%s", field_type, field_len)
            break
        fields[field_type] = frame[value_start:value_end]
        offset = value_end

    transponder_code = None
    if AMB_F_TRAN_CODE in fields:
        transponder_code = decode_transponder_code(fields[AMB_F_TRAN_CODE])

    transponder_number = None
    if len(fields.get(AMB_F_TRANSPONDER, b"")) == 4:
        transponder_number = _u32le(fields[AMB_F_TRANSPONDER])

    long_id = transponder_code or (str(transponder_number) if transponder_number is not None else None)
    if not long_id:
        log.debug("AMB P3 Passing ohne Transponder-ID verworfen")
        return

    time_field = fields.get(AMB_F_UTC_TIME) or fields.get(AMB_F_RTC_TIME)
    if not time_field or len(time_field) != 8:
        log.debug("AMB P3 Passing ohne gültige Zeit verworfen: %s", long_id)
        return

    if AMB_F_UTC_TIME in fields:
        passing_dt = micros_to_datetime(_u64le(time_field))
        time_source = "utc"
    else:
        passing_dt = rtc_micros_to_datetime(_u64le(time_field))
        time_source = "rtc_local"
    metadata = {
        "chip_numeric_id": transponder_number,
        "passing_number": _u32le(fields[AMB_F_PASSING_NUMBER]) if len(fields.get(AMB_F_PASSING_NUMBER, b"")) == 4 else None,
        "strength": _u16le(fields[AMB_F_STRENGTH]) if len(fields.get(AMB_F_STRENGTH, b"")) == 2 else None,
        "hits": _u16le(fields[AMB_F_HITS]) if len(fields.get(AMB_F_HITS, b"")) == 2 else None,
        "decoder_id": _u32le(fields[AMB_F_DECODER_ID]) if len(fields.get(AMB_F_DECODER_ID, b"")) == 4 else None,
        "time_source": time_source,
        "protocol": "amb_p3",
    }

    log.info(
        "AMB P3 Passing: LongID=%s Zeit=%s Strength=%s Hits=%s",
        long_id,
        passing_dt.isoformat(),
        metadata.get("strength"),
        metadata.get("hits"),
    )
    handle_passing(long_id, passing_dt, metadata)


def parse_amb_p3_stream(sock: socket.socket):
    buf = b""
    while not shutdown_event.is_set():
        try:
            chunk = sock.recv(512)
            if not chunk:
                log.warning("Decoder hat Verbindung getrennt")
                break
            buf += chunk

            while True:
                sor_pos = buf.find(bytes([AMB_SOR]))
                if sor_pos == -1:
                    if buf:
                        log.debug("Nicht-AMB-Daten verworfen: %s", buf[:32].hex())
                    buf = b""
                    break
                if sor_pos > 0:
                    log.debug("Daten vor AMB-SOR verworfen: %s", buf[:sor_pos].hex())
                    buf = buf[sor_pos:]

                eor_pos = buf.find(bytes([AMB_EOR]), 1)
                if eor_pos == -1:
                    break

                frame = buf[:eor_pos + 1]
                buf = buf[eor_pos + 1:]
                parse_amb_p3_record(frame)

        except socket.timeout:
            continue
        except OSError as e:
            log.error("Socket-Fehler: %s", e)
            break


def send_amb_p3_startup(sock: socket.socket):
    """Optionale Startsequenz für Decoder, die erst danach Passings streamen."""
    if not DECODER_STARTUP_HEX:
        return

    try:
        data = bytes.fromhex(DECODER_STARTUP_HEX)
    except ValueError:
        log.warning("decoder.startup_hex ist ungültig und wird ignoriert")
        return

    sock.sendall(data)
    log.info("AMB P3 Startsequenz gesendet (%d Bytes)", len(data))


# ---------------------------------------------------------------------------
# DCI-Protokoll Parser
#
# Ältere ProChip Decoder senden binäre Frames über TCP:
#
#   [0x02] [MSG_ID 1B] [LENGTH 2B big-endian] [DATA ...] [0x03] [CRC16 2B]
#
# MSG_ID 0x03 = Transponder Passing
# Data-Layout (Passing):
#   Bytes 0-4  : Transponder Long ID (5 Bytes, Big-Endian, als Hex-String dargestellt)
#   Bytes 5-8  : Timestamp Sekunden seit 1.1.2000 (4B big-endian)
#   Bytes 9-10 : Millisekunden (2B big-endian)
#   Byte  11   : Loop-/Antennen-ID
#
# Hinweis: Das genaue Format kann je nach Decoder-Firmware variieren.
# Falls dein Decoder ein anderes Format sendet, bitte melden - dann passen
# wir den Parser hier an.
# ---------------------------------------------------------------------------

STX = 0x02
ETX = 0x03
MSG_PASSING = 0x03

EPOCH_OFFSET = 946684800  # Sekunden zwischen 1.1.1970 und 1.1.2000


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def parse_dci_stream(sock: socket.socket):
    buf = b""
    while not shutdown_event.is_set():
        try:
            chunk = sock.recv(256)
            if not chunk:
                log.warning("Decoder hat Verbindung getrennt")
                break
            buf += chunk

            while True:
                stx_pos = buf.find(bytes([STX]))
                if stx_pos == -1:
                    buf = b""
                    break
                buf = buf[stx_pos:]

                # Mindest-Frame-Länge: STX(1) + MSG_ID(1) + LEN(2) + ETX(1) + CRC(2) = 7
                if len(buf) < 7:
                    break

                msg_id = buf[1]
                length = struct.unpack(">H", buf[2:4])[0]
                frame_end = 4 + length + 1 + 2  # data + ETX + CRC

                if len(buf) < frame_end:
                    break

                etx_pos = 4 + length
                if buf[etx_pos] != ETX:
                    # Synchronisation verloren - ein Byte vorwärts
                    buf = buf[1:]
                    continue

                frame_data = buf[4:4 + length]
                crc_received = struct.unpack(">H", buf[etx_pos + 1:etx_pos + 3])[0]
                crc_calc = crc16(buf[1:etx_pos + 1])

                buf = buf[frame_end:]

                if crc_received != crc_calc:
                    log.debug("CRC-Fehler - Frame verworfen")
                    continue

                if msg_id == MSG_PASSING and len(frame_data) >= 12:
                    long_id_bytes = frame_data[0:5]
                    long_id = long_id_bytes.hex().upper()

                    ts_sec = struct.unpack(">I", frame_data[5:9])[0]
                    ts_ms = struct.unpack(">H", frame_data[9:11])[0]
                    unix_ts = EPOCH_OFFSET + ts_sec + ts_ms / 1000.0
                    passing_dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)

                    log.info("Passing: LongID=%s Zeit=%s", long_id, passing_dt.isoformat())
                    handle_passing(long_id, passing_dt)

        except socket.timeout:
            continue
        except OSError as e:
            log.error("Socket-Fehler: %s", e)
            break


def send_hello(sock: socket.socket):
    """DCI-Handshake: HELLO-Nachricht senden."""
    hostname = socket.gethostname().encode("ascii")[:16]
    data = hostname.ljust(16, b"\x00")
    length = len(data)
    frame = bytes([STX, 0x01]) + struct.pack(">H", length) + data + bytes([ETX])
    frame += struct.pack(">H", crc16(frame[1:-1]))
    sock.sendall(frame)
    log.info("HELLO gesendet an Decoder")


# ---------------------------------------------------------------------------
# Decoder-Verbindung (mit Auto-Reconnect)
# ---------------------------------------------------------------------------

def decoder_worker():
    while not shutdown_event.is_set():
        log.info(
            "Verbinde mit Decoder %s:%s (%s) ...",
            DECODER_HOST,
            DECODER_PORT,
            DECODER_PROTOCOL,
        )
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(10)
                sock.connect((DECODER_HOST, DECODER_PORT))
                sock.settimeout(2)
                log.info("Verbunden mit Decoder")
                if DECODER_PROTOCOL == "dci":
                    send_hello(sock)
                    parse_dci_stream(sock)
                else:
                    send_amb_p3_startup(sock)
                    parse_amb_p3_stream(sock)
        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            log.warning("Decoder nicht erreichbar: %s", e)

        if not shutdown_event.is_set():
            log.info("Reconnect in %ss ...", RECONNECT_INTERVAL)
            shutdown_event.wait(RECONNECT_INTERVAL)


# ---------------------------------------------------------------------------
# Simulation (zum Testen ohne Decoder)
# ---------------------------------------------------------------------------

DEMO_TRANSPONDERS = [
    ("1000000001AB", "1"),
    ("1000000002CD", "2"),
    ("1000000003EF", "3"),
    ("1000000004GH", "4"),
    ("1000000005IJ", "5"),
    ("1000000006KL", "6"),
    ("1000000007MN", "7"),
    ("1000000008OP", "8"),
    ("1000000009QR", "9"),
    ("1000000010ST", "10"),
    ("1000000011UV", "11"),
    ("1000000012WX", "12"),
    ("1000000013YZ", "13"),
    ("1000000014AA", "14"),
    ("1000000015BB", "15"),
    ("1000000016CC", "16"),
    ("1000000017DD", "17"),
    ("1000000018EE", "18"),
    ("1000000019FF", "19"),
    ("1000000020GG", "20"),
    ("1000000021HH", "21"),
    ("1000000022II", "22"),
    ("1000000023JJ", "23"),
    ("1000000024KK", "24"),
    ("1000000025LL", "25"),
    ("1000000026MM", "26"),
    ("1000000027NN", "27"),
    ("1000000028OO", "28"),
    ("1000000029PP", "29"),
    ("1000000030QQ", "30"),
    ("1000000031RR", "31"),
    ("1000000032SS", "32"),
    ("1000000033TT", "33"),
    ("1000000034UU", "34"),
    ("1000000035VV", "35"),
    ("1000000036WW", "36"),
]


def simulation_worker():
    log.info("SIMULATIONSMODUS aktiv - kein echter Decoder")
    # Registry mit Demo-Einträgen befüllen
    with registry_lock:
        for long_id, short_id in DEMO_TRANSPONDERS:
            registry[long_id] = short_id

    real_lap_seconds = (SIM_LAP_LENGTH_KM / max(SIM_AVERAGE_SPEED_KMH, 1.0)) * 3600.0
    lap_interval = max(1.0, real_lap_seconds / max(SIM_SPEED_FACTOR, 1.0))
    tick_interval = min(0.2, max(0.05, lap_interval / max(len(DEMO_TRANSPONDERS), 1) / 2))
    emit_window = max(0.08, tick_interval * 1.8)
    log.info(
        "[SIM] %.2f km Runde bei %.1f km/h, Faktor %.1fx => %.2fs pro simulierter Runde",
        SIM_LAP_LENGTH_KM,
        SIM_AVERAGE_SPEED_KMH,
        SIM_SPEED_FACTOR,
        lap_interval,
    )
    start = time.time()
    last_emit = {}

    while not shutdown_event.is_set():
        now = time.time()
        elapsed = now - start

        for i, (long_id, short_id) in enumerate(DEMO_TRANSPONDERS):
            # Jeder Fahrer leicht versetzt
            offset = i * (lap_interval / len(DEMO_TRANSPONDERS))
            lap_no = int((elapsed - offset) // lap_interval)
            if lap_no >= 0 and (elapsed - offset) % lap_interval < emit_window and last_emit.get(long_id) != lap_no:
                last_emit[long_id] = lap_no
                passing_dt = datetime.now(timezone.utc)
                log.info("[SIM] Passing: LongID=%s ShortID=%s", long_id, short_id)
                handle_passing(long_id, passing_dt)

        shutdown_event.wait(tick_interval)


# ---------------------------------------------------------------------------
# Graceful Shutdown
# ---------------------------------------------------------------------------

shutdown_event = threading.Event()


def on_signal(sig, frame):
    log.info("Signal %s empfangen - beende Bridge ...", sig)
    shutdown_event.set()
    sender_wakeup.set()


signal.signal(signal.SIGINT, on_signal)
signal.signal(signal.SIGTERM, on_signal)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global buffer_conn

    log.info("=== MYLAPS Bridge gestartet ===")
    log.info("Server-Weiterleitung: %s", "aktiv" if SERVER_ENABLED else "deaktiviert")
    if SERVER_ENABLED:
        log.info("Server: %s", API_URL)
        log.info("Batch-Server: %s", BATCH_API_URL)
    log.info("Decoder-Protokoll: %s", DECODER_PROTOCOL)
    if LOCAL_TIMING_ENABLED and local_timing:
        local_timing.init_db(LOCAL_TIMING_DB)
        log.info("Lokales Dashboard aktiv: %s", LOCAL_TIMING_DB)
    elif LOCAL_TIMING_ENABLED:
        log.warning("Lokales Dashboard ist aktiviert, aber local_timing.py konnte nicht importiert werden")

    if not os.path.exists(CONFIG_PATH):
        log.error("config.ini nicht gefunden: %s", CONFIG_PATH)
        sys.exit(1)

    if SERVER_ENABLED and API_KEY in ("HIER_DEINEN_KEY_EINTRAGEN", ""):
        log.error("Bitte API_KEY in config.ini eintragen")
        sys.exit(1)

    buffer_conn = init_buffer(BUFFER_DB)
    log.info("Offline-Puffer: %s", BUFFER_DB)
    recover_local_pending()

    threads = []

    # Vor dem Decoderstart einmal synchron laden. Selbst bei einem Fehler werden
    # neue Passings dauerhaft als UNMAPPED gespeichert und spaeter nachgezogen.
    load_registry()
    reg_thread = threading.Thread(target=registry_worker, daemon=True, name="registry")
    reg_thread.start()
    threads.append(reg_thread)

    if SERVER_ENABLED:
        if HTTP_WORKERS != 1:
            log.warning(
                "http_workers=%s wird aus Gruenden der Passing-Reihenfolge auf 1 begrenzt",
                HTTP_WORKERS,
            )
        http_thread = threading.Thread(
            target=http_sender_worker,
            args=(1,),
            daemon=True,
            name="http-ordered",
        )
        http_thread.start()
        threads.append(http_thread)

    # Decoder oder Simulation
    if SIMULATE:
        worker = threading.Thread(target=simulation_worker, daemon=True, name="simulator")
    else:
        worker = threading.Thread(target=decoder_worker, daemon=True, name="decoder")
    worker.start()
    threads.append(worker)

    log.info("Bridge läuft. Ctrl+C zum Beenden.")
    shutdown_event.wait()

    sender_wakeup.set()
    for thread in threads:
        thread.join(timeout=max(3, HTTP_TIMEOUT + 2))
    if buffer_conn:
        with buffer_lock:
            buffer_conn.commit()
            buffer_conn.close()
    log.info("Bridge beendet.")


if __name__ == "__main__":
    main()
