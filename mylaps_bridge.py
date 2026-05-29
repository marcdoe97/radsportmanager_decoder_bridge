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
import os
import queue
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
HTTP_WORKERS = config.getint("bridge", "http_workers", fallback=3)
SIM_AVERAGE_SPEED_KMH = config.getfloat("bridge", "simulation_average_speed_kmh", fallback=45.0)
SIM_SPEED_FACTOR = config.getfloat("bridge", "simulation_speed_factor", fallback=10.0)
SIM_LAP_LENGTH_KM = config.getfloat("bridge", "simulation_lap_length_km", fallback=1.0)
LOCAL_TIMING_ENABLED = config.getboolean("local", "enabled", fallback=True)
LOCAL_TIMING_DB = config.get("local", "db", fallback="local_timing.db")

if not os.path.isabs(BUFFER_DB):
    BUFFER_DB = os.path.join(os.path.dirname(__file__), BUFFER_DB)
if not os.path.isabs(LOCAL_TIMING_DB):
    LOCAL_TIMING_DB = os.path.join(os.path.dirname(__file__), LOCAL_TIMING_DB)

if not BATCH_API_URL and API_URL:
    parts = urlsplit(API_URL)
    BATCH_API_URL = urlunsplit(
        (parts.scheme, parts.netloc, parts.path.replace("passing.php", "passing_batch.php"), parts.query, parts.fragment)
    )

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
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
    conn.execute(
        """CREATE TABLE IF NOT EXISTS buffer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            attempts INTEGER DEFAULT 0
        )"""
    )
    conn.commit()
    return conn


def buffer_put(conn: sqlite3.Connection, payload: dict):
    conn.execute(
        "INSERT INTO buffer (payload, created_at) VALUES (?, ?)",
        (json.dumps(payload), datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def buffer_flush(conn: sqlite3.Connection):
    """Versucht gepufferte Passings erneut zu senden."""
    with buffer_lock:
        rows = conn.execute(
            "SELECT id, payload FROM buffer WHERE attempts < 10 ORDER BY id LIMIT ?",
            (BATCH_SIZE,),
        ).fetchall()
    if not rows:
        return

    row_ids = []
    payloads = []
    for row_id, payload_str in rows:
        try:
            payloads.append(json.loads(payload_str))
            row_ids.append(row_id)
        except ValueError:
            with buffer_lock:
                conn.execute("DELETE FROM buffer WHERE id = ?", (row_id,))
                conn.commit()

    ok = send_batch_to_server(payloads)
    with buffer_lock:
        if ok:
            conn.executemany("DELETE FROM buffer WHERE id = ?", [(row_id,) for row_id in row_ids])
        else:
            conn.executemany(
                "UPDATE buffer SET attempts = attempts + 1 WHERE id = ?",
                [(row_id,) for row_id in row_ids],
            )
        conn.commit()


# ---------------------------------------------------------------------------
# Transponder-Registry (Long ID -> Short ID)
# ---------------------------------------------------------------------------

registry: dict[str, str] = {}
registry_lock = threading.Lock()


def load_registry():
    global registry
    if SERVER_ENABLED and REGISTRY_URL:
        try:
            resp = requests.get(
                REGISTRY_URL,
                headers={"X-API-Key": API_KEY},
                timeout=HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            with registry_lock:
                registry = {entry["long_id"]: entry["short_id"] for entry in data}
            log.info("Transponder-Registry geladen: %d Einträge", len(registry))
        except Exception as e:
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


def registry_worker():
    while not shutdown_event.is_set():
        load_registry()
        shutdown_event.wait(REGISTRY_REFRESH)


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
payload_queue: queue.Queue[dict] = queue.Queue(maxsize=QUEUE_MAX_SIZE)


def send_to_server(payload: dict, use_buffer: bool = True) -> bool:
    if not SERVER_ENABLED:
        return True
    try:
        resp = requests.post(
            API_URL,
            json=payload,
            headers={"X-API-Key": API_KEY},
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                data = {}

            if data.get("status") == "UNRESOLVED":
                log.warning(
                    "Passing gesendet, aber Server konnte Chip nicht zuordnen: short_id=%s",
                    payload.get("short_id"),
                )
            elif data.get("duplicate"):
                log.info("Passing war bereits bekannt: %s", payload.get("event_id", ""))
            else:
                log.info(
                    "Passing akzeptiert: short_id=%s processed=%s",
                    payload.get("short_id"),
                    data.get("processed", []),
                )
            return True
        else:
            log.warning("Server antwortete %s: %s", resp.status_code, resp.text[:200])
            return False
    except Exception as e:
        log.warning("Netzwerkfehler beim Senden: %s", e)
        if use_buffer and buffer_conn:
            with buffer_lock:
                buffer_put(buffer_conn, payload)
            log.info("Passing gepuffert (offline): %s", payload.get("event_id", ""))
        return False


def send_batch_to_server(payloads: list[dict]) -> bool:
    if not payloads:
        return True
    if not SERVER_ENABLED:
        return True
    try:
        resp = requests.post(
            BATCH_API_URL,
            json={"passings": payloads},
            headers={"X-API-Key": API_KEY},
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            log.warning("Batch-Endpoint antwortete %s: %s", resp.status_code, resp.text[:200])
            return False

        try:
            data = resp.json()
        except ValueError:
            data = {}

        if not data.get("success", False):
            log.warning("Batch wurde vom Server abgelehnt: %s", data)
            return False

        log.info(
            "Batch gesendet: received=%s accepted=%s ignored=%s unresolved=%s duplicate=%s",
            data.get("received", len(payloads)),
            data.get("accepted", 0),
            data.get("ignored", 0),
            data.get("unresolved", 0),
            data.get("duplicate", 0),
        )
        return True
    except Exception as e:
        log.warning("Netzwerkfehler beim Batch-Senden: %s", e)
        return False


def enqueue_payload(payload: dict):
    try:
        payload_queue.put_nowait(payload)
    except queue.Full:
        if buffer_conn:
            with buffer_lock:
                buffer_put(buffer_conn, payload)
            log.warning("HTTP-Queue voll, Passing lokal gepuffert: %s", payload.get("event_id", ""))
        else:
            log.error("HTTP-Queue voll und Puffer nicht bereit: %s", payload.get("event_id", ""))


def http_sender_worker(worker_id: int):
    while not shutdown_event.is_set() or not payload_queue.empty():
        batch = []
        try:
            first_payload = payload_queue.get(timeout=0.2)
        except queue.Empty:
            continue

        batch.append(first_payload)
        deadline = time.monotonic() + BATCH_FLUSH_INTERVAL
        while len(batch) < BATCH_SIZE and time.monotonic() < deadline:
            timeout = max(0.0, deadline - time.monotonic())
            try:
                batch.append(payload_queue.get(timeout=timeout))
            except queue.Empty:
                break

        ok = send_batch_to_server(batch)
        if not ok and buffer_conn:
            with buffer_lock:
                for payload in batch:
                    buffer_put(buffer_conn, payload)
            log.warning("HTTP-Worker %s hat %d Passings lokal gepuffert", worker_id, len(batch))

        for _ in batch:
            payload_queue.task_done()


def build_payload(
    long_id: str,
    short_id: str,
    passing_time_iso: str,
    metadata: dict | None = None,
) -> dict:
    event_id = hashlib.sha256(
        f"{long_id}:{passing_time_iso}".encode()
    ).hexdigest()[:32]
    payload = {
        "event_id": event_id,
        "chip_long_id": long_id,
        "short_id": short_id,
        "passing_time": passing_time_iso,
    }
    if metadata:
        payload.update({k: v for k, v in metadata.items() if v is not None})
    return payload


def handle_passing(long_id: str, passing_dt: datetime, metadata: dict | None = None):
    short_id = resolve_long_id(long_id)
    if short_id is None:
        log.warning("Unbekannte Long ID: %s - nicht in Registry", long_id)
        return

    passing_time_iso = passing_dt.astimezone(timezone.utc).isoformat()
    log.info("LongID %s -> ShortID %s", long_id, short_id)
    payload = build_payload(long_id, short_id, passing_time_iso, metadata)
    if LOCAL_TIMING_ENABLED and local_timing:
        try:
            local_timing.record_passing(LOCAL_TIMING_DB, payload)
        except Exception as e:
            log.warning("Lokales Speichern des Passings fehlgeschlagen: %s", e)
    if SERVER_ENABLED:
        enqueue_payload(payload)


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

    passing_dt = micros_to_datetime(_u64le(time_field))
    metadata = {
        "chip_numeric_id": transponder_number,
        "passing_number": _u32le(fields[AMB_F_PASSING_NUMBER]) if len(fields.get(AMB_F_PASSING_NUMBER, b"")) == 4 else None,
        "strength": _u16le(fields[AMB_F_STRENGTH]) if len(fields.get(AMB_F_STRENGTH, b"")) == 2 else None,
        "hits": _u16le(fields[AMB_F_HITS]) if len(fields.get(AMB_F_HITS, b"")) == 2 else None,
        "decoder_id": _u32le(fields[AMB_F_DECODER_ID]) if len(fields.get(AMB_F_DECODER_ID, b"")) == 4 else None,
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
# Buffer-Flush Worker
# ---------------------------------------------------------------------------

def flush_worker():
    while not shutdown_event.is_set():
        if buffer_conn:
            buffer_flush(buffer_conn)
        shutdown_event.wait(30)


# ---------------------------------------------------------------------------
# Graceful Shutdown
# ---------------------------------------------------------------------------

shutdown_event = threading.Event()


def on_signal(sig, frame):
    log.info("Signal %s empfangen - beende Bridge ...", sig)
    shutdown_event.set()


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

    threads = []

    # Registry laden
    reg_thread = threading.Thread(target=registry_worker, daemon=True, name="registry")
    reg_thread.start()
    threads.append(reg_thread)

    # Kurz warten bis Registry geladen
    time.sleep(2)

    if SERVER_ENABLED:
        # Puffer-Flush
        flush_thread = threading.Thread(target=flush_worker, daemon=True, name="flush")
        flush_thread.start()
        threads.append(flush_thread)

        for worker_id in range(max(1, HTTP_WORKERS)):
            http_thread = threading.Thread(
                target=http_sender_worker,
                args=(worker_id + 1,),
                daemon=True,
                name=f"http-{worker_id + 1}",
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

    log.info("Bridge beendet.")


if __name__ == "__main__":
    main()
