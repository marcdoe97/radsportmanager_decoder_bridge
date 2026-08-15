"""Streamlit dashboard for local MYLAPS race timing."""

from __future__ import annotations

import io
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

import local_timing


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "local_timing.db"
BUFFER_DB_PATH = BASE_DIR / "buffer.db"


def ensure_streamlit_runtime() -> None:
    """Restart through `streamlit run` when the file was opened with Python."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        if get_script_run_ctx() is not None:
            return
    except Exception:
        return

    if os.environ.get("RSM_STREAMLIT_DIRECT_LAUNCH") == "1":
        return

    print("Starte Dashboard mit Streamlit auf http://localhost:8501 ...")
    env = os.environ.copy()
    env["RSM_STREAMLIT_DIRECT_LAUNCH"] = "1"
    subprocess.call(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(Path(__file__).resolve()),
            "--server.port",
            "8501",
        ],
        env=env,
    )
    raise SystemExit


ensure_streamlit_runtime()


def read_uploaded_table(uploaded_file) -> list[dict]:
    suffix = Path(uploaded_file.name).suffix.lower()
    data = uploaded_file.getvalue()
    if suffix in {".xlsx", ".xls"}:
        frame = pd.read_excel(io.BytesIO(data), dtype=str).fillna("")
    else:
        frame = pd.read_csv(io.BytesIO(data), dtype=str, sep=None, engine="python").fillna("")
    return frame.to_dict(orient="records")


def load_frame(query: str, params: tuple = ()) -> pd.DataFrame:
    with local_timing.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=params)


def format_lap_time(seconds: float | None) -> str:
    if seconds is None or pd.isna(seconds):
        return ""
    minutes = int(seconds // 60)
    rest = seconds - minutes * 60
    return f"{minutes}:{rest:05.2f}"


def dashboard_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    drivers = load_frame(
        """
        SELECT start_number, name, team, short_id, category
        FROM drivers
        ORDER BY CAST(start_number AS INTEGER), start_number
        """
    )
    passings = load_frame(
        """
        SELECT event_id, chip_long_id, short_id, passing_time, status, metadata
        FROM passings
        WHERE status = 'ACTIVE'
        ORDER BY passing_time
        """
    )
    if passings.empty:
        standings = drivers.copy()
        standings["runden"] = 0
        standings["letzte_durchfahrt"] = ""
        standings["letzte_rundenzeit"] = ""
        standings["runden_rueckstand"] = 0
        return standings, passings

    passings["passing_dt"] = pd.to_datetime(passings["passing_time"], utc=True, errors="coerce")
    grouped = passings.dropna(subset=["passing_dt"]).sort_values("passing_dt").groupby("short_id")
    stats = grouped.agg(
        runden=("event_id", "count"),
        letzte_durchfahrt=("passing_dt", "max"),
    ).reset_index()
    lap_times = []
    for short_id, group in grouped:
        diffs = group["passing_dt"].diff().dt.total_seconds().dropna()
        lap_times.append({"short_id": short_id, "last_lap_seconds": diffs.iloc[-1] if not diffs.empty else None})
    lap_frame = pd.DataFrame(lap_times)

    standings = drivers.merge(stats, on="short_id", how="outer")
    if not lap_frame.empty:
        standings = standings.merge(lap_frame, on="short_id", how="left")
    else:
        standings["last_lap_seconds"] = None

    for column in ["start_number", "name", "team", "category"]:
        standings[column] = standings[column].fillna("")
    standings["runden"] = standings["runden"].fillna(0).astype(int)
    max_laps = int(standings["runden"].max()) if not standings.empty else 0
    standings["runden_rueckstand"] = max_laps - standings["runden"]
    standings["letzte_rundenzeit"] = standings["last_lap_seconds"].apply(format_lap_time)
    standings["letzte_durchfahrt"] = standings["letzte_durchfahrt"].apply(
        lambda value: value.strftime("%H:%M:%S") if pd.notna(value) else ""
    )
    standings = standings.sort_values(
        ["runden", "letzte_durchfahrt", "start_number"],
        ascending=[False, True, True],
    )
    return standings, passings


def render_imports() -> None:
    st.subheader("Import")
    col_a, col_b = st.columns(2)

    with col_a:
        drivers_file = st.file_uploader("Fahrer Excel/CSV", type=["csv", "xlsx", "xls"], key="drivers")
        replace_drivers = st.checkbox("Fahrer ersetzen", value=True)
        if st.button("Fahrer importieren", disabled=drivers_file is None):
            count = local_timing.import_drivers(DB_PATH, read_uploaded_table(drivers_file), replace=replace_drivers)
            st.success(f"{count} Fahrer importiert")
            st.rerun()

    with col_b:
        mapping_file = st.file_uploader("Transponder-Mapping Excel/CSV", type=["csv", "xlsx", "xls"], key="mapping")
        replace_mapping = st.checkbox("Mapping ersetzen", value=True)
        if st.button("Mapping importieren", disabled=mapping_file is None):
            count = local_timing.import_mapping(DB_PATH, read_uploaded_table(mapping_file), replace=replace_mapping)
            st.success(f"{count} Mapping-Einträge importiert")
            st.rerun()

    with st.expander("Erwartete Spalten"):
        st.write("Fahrer: `Startnummer`, `Name`, `Team`, `Short_ID`, optional `Kategorie`")
        st.write("Mapping: `Long_ID`, `Short_ID`, optional `Bezeichnung`")


def render_control(passings: pd.DataFrame) -> None:
    st.subheader("Rundenprotokoll")
    latest = load_frame(
        """
        SELECT p.event_id, p.passing_time, p.short_id, d.start_number, d.name, d.team, p.chip_long_id, p.status
        FROM passings p
        LEFT JOIN drivers d ON d.short_id = p.short_id
        ORDER BY p.passing_time DESC
        LIMIT 300
        """
    )
    if latest.empty:
        st.info("Noch keine Durchfahrten.")
        return

    latest["Zeit"] = pd.to_datetime(latest["passing_time"], utc=True, errors="coerce").dt.strftime("%H:%M:%S")
    display = latest.rename(
        columns={
            "start_number": "Nr",
            "name": "Fahrer",
            "team": "Team",
            "short_id": "Short ID",
            "chip_long_id": "Long ID",
            "status": "Status",
        }
    )[["Zeit", "Nr", "Fahrer", "Team", "Short ID", "Long ID", "Status"]]
    st.dataframe(display, width="stretch", hide_index=True, height=420)

    with st.expander("Durchfahrt ignorieren / reaktivieren"):
        selected_event = st.selectbox("Event", latest["event_id"].tolist(), format_func=lambda event_id: event_id[:12])
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("Ignorieren"):
                with local_timing.connect(DB_PATH) as conn:
                    conn.execute("UPDATE passings SET status = 'IGNORED' WHERE event_id = ?", (selected_event,))
                st.rerun()
        with col_b:
            if st.button("Reaktivieren"):
                with local_timing.connect(DB_PATH) as conn:
                    conn.execute("UPDATE passings SET status = 'ACTIVE' WHERE event_id = ?", (selected_event,))
                st.rerun()


def render_bridge_diagnostics() -> None:
    st.subheader("Bridge- und Zustellstatus")
    delivery = load_frame(
        """
        SELECT delivery_status, COUNT(*) AS anzahl
        FROM passings
        GROUP BY delivery_status
        ORDER BY delivery_status
        """
    )
    if delivery.empty:
        st.info("Noch keine Passings erfasst.")
    else:
        columns = st.columns(max(1, min(5, len(delivery))))
        for index, row in delivery.iterrows():
            columns[index % len(columns)].metric(str(row["delivery_status"]), int(row["anzahl"]))

    status = load_frame(
        "SELECT registry_last_success, registry_last_error, registry_entries, updated_at FROM bridge_status WHERE id=1"
    )
    if not status.empty:
        row = status.iloc[0]
        st.write(
            f"Registry: **{int(row['registry_entries'])} EintrÃ¤ge**, "
            f"zuletzt erfolgreich: `{row['registry_last_success'] or '-'}`"
        )
        if row["registry_last_error"]:
            st.error(f"Letzter Registry-Fehler: {row['registry_last_error']}")

    pending = load_frame(
        """
        SELECT passing_time, chip_long_id, short_id, delivery_status,
               delivery_attempts, last_error, passing_number
        FROM passings
        WHERE delivery_status NOT IN ('ACKED','LOCAL_ONLY')
        ORDER BY passing_time DESC
        LIMIT 300
        """
    )
    if pending.empty:
        st.success("Keine offenen oder fehlgeschlagenen Zustellungen.")
    else:
        st.dataframe(
            pending.rename(
                columns={
                    "passing_time": "Zeit",
                    "chip_long_id": "Long ID",
                    "short_id": "Short ID",
                    "delivery_status": "Zustellung",
                    "delivery_attempts": "Versuche",
                    "last_error": "Fehler",
                    "passing_number": "Passing Nr.",
                }
            ),
            width="stretch",
            hide_index=True,
            height=420,
        )

    if BUFFER_DB_PATH.exists():
        try:
            with local_timing.connect(BUFFER_DB_PATH) as conn:
                buffer_counts = pd.read_sql_query(
                    "SELECT state, COUNT(*) AS anzahl FROM buffer GROUP BY state ORDER BY state",
                    conn,
                )
            if not buffer_counts.empty:
                st.caption("Dauerhafter Sendepuffer")
                st.dataframe(buffer_counts, width="stretch", hide_index=True)
                retry_col, failed_col = st.columns(2)
                with retry_col:
                    if st.button("Offene jetzt erneut versuchen"):
                        with local_timing.connect(BUFFER_DB_PATH) as conn:
                            conn.execute(
                                "UPDATE buffer SET next_attempt_at=0 WHERE state IN ('PENDING','RETRY','UNMAPPED')"
                            )
                        st.success("Offene Zustellungen wurden fuer den naechsten Bridge-Lauf freigegeben.")
                with failed_col:
                    if st.button("Abgelehnte erneut freigeben"):
                        with local_timing.connect(BUFFER_DB_PATH) as conn:
                            conn.execute(
                                "UPDATE buffer SET state='RETRY',next_attempt_at=0,last_error=NULL WHERE state='FAILED'"
                            )
                        st.warning("Dauerhaft abgelehnte Eintraege werden erneut geprueft.")
        except Exception as exc:
            st.warning(f"Sendepuffer konnte nicht gelesen werden: {exc}")

    gaps = load_frame(
        """SELECT detected_at,decoder_id,previous_passing_number,current_passing_number,
                  missing_count,previous_passing_time,current_passing_time
           FROM decoder_gaps ORDER BY detected_at DESC,id DESC LIMIT 100"""
    )
    if not gaps.empty:
        st.error(f"Erkannte Decoder-Sequenzluecken: {len(gaps)} (letzte 100)")
        st.dataframe(
            gaps.rename(
                columns={
                    "detected_at": "Erkannt",
                    "decoder_id": "Decoder",
                    "previous_passing_number": "Vorherige Nr.",
                    "current_passing_number": "Aktuelle Nr.",
                    "missing_count": "Fehlende Nummern",
                    "previous_passing_time": "Vorherige Zeit",
                    "current_passing_time": "Aktuelle Zeit",
                }
            ),
            width="stretch",
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(page_title="Radsportmanager Decoder Dashboard", layout="wide")
    local_timing.init_db(DB_PATH)

    st.title("Decoder Dashboard")
    st.caption("Lokales Kampfgericht-Dashboard für den direkt angeschlossenen MYLAPS Decoder")

    st.sidebar.header("Aktualisierung")
    refresh_seconds = st.sidebar.number_input("Sekunden", min_value=5, max_value=300, value=60, step=5)
    st.sidebar.write(f"Datenbank: `{DB_PATH.name}`")
    st.sidebar.button("Jetzt aktualisieren")
    st.markdown(f"<meta http-equiv='refresh' content='{int(refresh_seconds)}'>", unsafe_allow_html=True)

    standings, passings = dashboard_data()
    active_passings = len(passings)
    known_drivers = int((standings["name"] != "").sum()) if not standings.empty else 0
    last_seen = ""
    if active_passings:
        last_dt = pd.to_datetime(passings["passing_time"], utc=True, errors="coerce").max()
        if pd.notna(last_dt):
            last_seen = last_dt.strftime("%H:%M:%S")

    metric_a, metric_b, metric_c = st.columns(3)
    metric_a.metric("Fahrer", known_drivers)
    metric_b.metric("Durchfahrten", active_passings)
    metric_c.metric("Letztes Signal", last_seen or "-")

    tabs = st.tabs(["Dashboard", "Rundenprotokoll", "Bridge-Status", "Import"])
    with tabs[0]:
        st.subheader("Live-Stand")
        visible = standings.rename(
            columns={
                "start_number": "Nr",
                "name": "Fahrer",
                "team": "Team",
                "category": "Kategorie",
                "short_id": "Short ID",
                "runden": "Runden",
                "letzte_durchfahrt": "Letzte Durchfahrt",
                "letzte_rundenzeit": "Letzte Rundenzeit",
                "runden_rueckstand": "Rückstand",
            }
        )
        st.dataframe(
            visible[["Nr", "Fahrer", "Team", "Kategorie", "Short ID", "Runden", "Letzte Rundenzeit", "Letzte Durchfahrt", "Rückstand"]],
            width="stretch",
            hide_index=True,
            height=520,
        )

    with tabs[1]:
        render_control(passings)

    with tabs[2]:
        render_bridge_diagnostics()

    with tabs[3]:
        render_imports()


if __name__ == "__main__":
    main()
