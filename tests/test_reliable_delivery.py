from __future__ import annotations

import json
import sqlite3
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import local_timing
import mylaps_bridge as bridge

TEST_DIR = Path(__file__).resolve().parent


@contextmanager
def temporary_db(name: str):
    path = TEST_DIR / name
    related = [path, Path(str(path) + "-wal"), Path(str(path) + "-shm")]
    for item in related:
        item.unlink(missing_ok=True)
    try:
        yield path
    finally:
        for item in related:
            item.unlink(missing_ok=True)


def payload(event_id: str, short_id: str = "DS-111") -> dict:
    return {
        "event_id": event_id,
        "chip_long_id": "CT-61033",
        "short_id": short_id,
        "passing_time": "2026-08-09T10:00:00+00:00",
        "decoder_id": 123,
        "passing_number": 456,
    }


class ReliableBufferTests(unittest.TestCase):
    def test_old_buffer_schema_is_migrated_without_losing_payload(self) -> None:
        with temporary_db("_test_old_buffer.db") as path:
            old_payload = payload("legacy-event")
            seed = sqlite3.connect(path)
            try:
                seed.execute(
                    "CREATE TABLE buffer (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, created_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0)"
                )
                seed.execute(
                    "INSERT INTO buffer(payload,created_at,attempts) VALUES(?,?,?)",
                    (json.dumps(old_payload), "2026-08-09T10:00:01+00:00", 10),
                )
                seed.commit()
            finally:
                seed.close()

            conn = bridge.init_buffer(str(path))
            row = conn.execute(
                "SELECT event_id,state,attempts,short_id FROM buffer"
            ).fetchone()
            self.assertEqual("legacy-event", row["event_id"])
            self.assertEqual("PENDING", row["state"])
            self.assertEqual(10, row["attempts"])
            self.assertEqual("DS-111", row["short_id"])
            conn.close()

    def test_per_item_ack_only_deletes_confirmed_passing(self) -> None:
        with temporary_db("_test_delivery_buffer.db") as path:
            conn = bridge.init_buffer(str(path))
            bridge.buffer_put(conn, payload("event-1"))
            bridge.buffer_put(conn, payload("event-2"))
            outcomes = {
                "event-1": {"disposition": "ACK", "reason": ""},
                "event-2": {"disposition": "RETRY", "reason": "temporarily unresolved"},
            }
            with mock.patch.object(bridge, "send_batch_to_server", return_value=outcomes), mock.patch.object(
                bridge, "_mark_local_delivery"
            ):
                bridge.buffer_flush(conn)

            rows = conn.execute(
                "SELECT event_id,state,attempts,last_error FROM buffer ORDER BY event_id"
            ).fetchall()
            self.assertEqual(1, len(rows))
            self.assertEqual("event-2", rows[0]["event_id"])
            self.assertEqual("RETRY", rows[0]["state"])
            self.assertEqual(1, rows[0]["attempts"])
            self.assertIn("unresolved", rows[0]["last_error"])
            conn.close()

    def test_unknown_transponder_stays_in_durable_buffer(self) -> None:
        with temporary_db("_test_unmapped_buffer.db") as path:
            conn = bridge.init_buffer(str(path))
            bridge.buffer_put(conn, payload("unmapped", short_id=""), state="UNMAPPED")
            with mock.patch.object(bridge, "resolve_long_id", return_value=None), mock.patch.object(
                bridge, "_mark_local_delivery"
            ), mock.patch.object(bridge, "REGISTRY_REFRESH", 60):
                bridge.buffer_flush(conn)
            row = conn.execute(
                "SELECT state,attempts,last_error,next_attempt_at FROM buffer WHERE event_id='unmapped'"
            ).fetchone()
            self.assertEqual("UNMAPPED", row["state"])
            self.assertEqual(0, row["attempts"])
            self.assertIn("Registry", row["last_error"])
            self.assertGreater(row["next_attempt_at"], 0)
            conn.close()

    def test_old_batch_response_with_unresolved_is_not_acknowledged(self) -> None:
        outcomes = bridge.parse_server_outcomes(
            [payload("event-1")],
            {"success": True, "accepted": 0, "unresolved": 1, "errors": 0},
        )
        self.assertEqual("RETRY", outcomes["event-1"]["disposition"])

    def test_old_batch_response_with_silent_ignore_is_not_acknowledged(self) -> None:
        outcomes = bridge.parse_server_outcomes(
            [payload("event-1")],
            {"success": True, "accepted": 0, "ignored": 1, "unresolved": 0, "errors": 0},
        )
        self.assertEqual("RETRY", outcomes["event-1"]["disposition"])

    def test_decoder_event_id_is_stable_but_includes_timestamp(self) -> None:
        metadata = {"decoder_id": 7, "passing_number": 42}
        first = bridge.build_payload("CT-1", "DS-1", "2026-08-09T10:00:00+00:00", metadata)
        duplicate = bridge.build_payload("CT-1", "DS-1", "2026-08-09T10:00:00+00:00", metadata)
        after_reboot = bridge.build_payload("CT-1", "DS-1", "2026-08-10T10:00:00+00:00", metadata)
        self.assertEqual(first["event_id"], duplicate["event_id"])
        self.assertNotEqual(first["event_id"], after_reboot["event_id"])


class LocalTimingMigrationTests(unittest.TestCase):
    def test_historic_rows_are_marked_unknown_not_resent(self) -> None:
        with temporary_db("_test_local_timing.db") as path:
            seed = sqlite3.connect(path)
            try:
                seed.execute(
                    """CREATE TABLE passings (
                        event_id TEXT PRIMARY KEY,
                        chip_long_id TEXT NOT NULL,
                        short_id TEXT NOT NULL,
                        passing_time TEXT NOT NULL,
                        metadata TEXT DEFAULT '{}',
                        status TEXT NOT NULL DEFAULT 'ACTIVE',
                        created_at TEXT NOT NULL
                    )"""
                )
                seed.execute(
                    "INSERT INTO passings VALUES(?,?,?,?,?,?,?)",
                    ("old", "CT-1", "DS-1", "2026-07-01T10:00:00+00:00", "{}", "ACTIVE", "2026-07-01"),
                )
                seed.commit()
            finally:
                seed.close()
            local_timing.init_db(path)
            check = local_timing.connect(path)
            try:
                status = check.execute(
                    "SELECT delivery_status FROM passings WHERE event_id='old'"
                ).fetchone()[0]
            finally:
                check.close()
            self.assertEqual("UNKNOWN", status)
            self.assertEqual([], local_timing.pending_delivery_payloads(path))

    def test_new_local_passing_can_rebuild_send_buffer_after_crash(self) -> None:
        with temporary_db("_test_local_recovery.db") as path:
            local_timing.record_passing(path, payload("recover-me"))
            recovered = local_timing.pending_delivery_payloads(path)
            self.assertEqual(1, len(recovered))
            self.assertEqual("recover-me", recovered[0]["event_id"])
            self.assertEqual(456, recovered[0]["passing_number"])

    def test_decoder_sequence_gap_is_persisted(self) -> None:
        with temporary_db("_test_decoder_gap.db") as path:
            first = payload("sequence-10")
            first["passing_number"] = 10
            first["passing_time"] = "2026-08-09T10:00:00+00:00"
            later = payload("sequence-13")
            later["passing_number"] = 13
            later["passing_time"] = "2026-08-09T10:00:03+00:00"
            self.assertIsNone(local_timing.record_passing(path, first))
            gap = local_timing.record_passing(path, later)
            self.assertIsNotNone(gap)
            self.assertEqual(2, gap["missing_count"])
            with local_timing.connect(path) as conn:
                stored = conn.execute(
                    "SELECT previous_passing_number,current_passing_number,missing_count FROM decoder_gaps"
                ).fetchone()
            self.assertEqual((10, 13, 2), tuple(stored))


if __name__ == "__main__":
    unittest.main()
