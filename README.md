# Radsportmanager Decoder Bridge

Lokale Bridge für MYLAPS ProChip Decoder und die Radsportmanager Webapp.

Dieses Repository enthält die lokale Komponente, die auf einem Windows-PC oder Raspberry Pi neben dem Decoder läuft. Die Bridge liest Transponder-Durchfahrten aus einem MYLAPS ProChip Decoder, ordnet Long-IDs zu Short-IDs zu, puffert Daten bei Netzwerkausfall und sendet die Durchfahrten an die Radsportmanager Webapp.

Die dazugehörige Webapp ist bereits vorhanden und kommuniziert mit dieser Bridge über HTTP-API-Endpunkte. Sie ist nicht Bestandteil dieses Repositories. Ich bin Eigentümer der Webapp und stelle sie Veranstaltern gegen Entgelt zur Miete bereit. Für produktive Nutzung, API-Zugang, Hosting und Einrichtung der Webapp bitte direkt Kontakt mit mir aufnehmen.

## Status

Die lokale Bridge ist als Arbeitsstand nutzbar, wenn eine passende Radsportmanager Webapp-Instanz und ein gültiger API-Key vorhanden sind.

Bereits enthalten:

- TCP-Verbindung zu MYLAPS ProChip Decodern
- AMB-P3-Parser für Smart Decoder mit `0x8E` / `0x8F` Frames
- DCI-Fallback für ältere Decoder
- Weiterleitung von Passings per HTTPS an die Radsportmanager Webapp
- Batch-Upload für dichte Zielpassagen
- Offline-Puffer in SQLite (`buffer.db`)
- automatische Nachlieferung gepufferter Passings
- Reconnect bei Decoder-Verbindungsabbruch
- Transponder-Registry-Cache: Long ID zu Short ID
- lokales Streamlit-Dashboard für Kampfgericht und Rundenprotokoll
- lokaler SQLite-Speicher (`local_timing.db`)
- Simulationsmodus ohne echten Decoder
- Windows-Startskripte
- Beispiel für systemd-Autostart auf Raspberry Pi

Noch erforderlich für produktiven Betrieb:

- Zugriff auf eine eingerichtete Radsportmanager Webapp-Instanz
- gültiger API-Key aus der Webapp
- Decoder-IP und Decoder-Port in `config.ini`
- Transponder-Mapping Long ID zu Short ID
- Fahrer-/Startlistenimport im lokalen Dashboard oder in der Webapp
- Praxistest mit dem konkret eingesetzten Decoder und dessen Firmware

## Architektur

```text
MYLAPS ProChip Decoder
  AMB P3, TCP Port 5403
  ältere DCI-Decoder ggf. TCP Port 3601
        |
        v
mylaps_bridge.py
  läuft auf Windows-PC oder Raspberry Pi
        |
        | HTTPS POST + API-Key
        v
Radsportmanager Webapp
  api/mylaps/passing.php
  api/mylaps/passing_batch.php
  api/mylaps/transponder_registry.php
        |
        v
Rennverwaltung, Live-Timing und Ergebnisverarbeitung
```

Der Modus wird in der Webapp pro Rennen gesetzt:

| Modus | Bedeutung |
|---|---|
| `MANUELL` | Kein Decoder, Ergebnisse werden manuell gepflegt |
| `MYLAPS_PC` | Bridge läuft auf dem Windows-PC am Decoder |
| `MYLAPS_PI` | Bridge läuft auf einem Raspberry Pi am Decoder |

`MYLAPS_PC` und `MYLAPS_PI` verwenden technisch dieselbe Bridge. Der Unterschied ist nur die Betriebsart vor Ort.

## Repository-Inhalt

| Datei | Beschreibung |
|---|---|
| `mylaps_bridge.py` | Hauptprogramm: Decoder-Listener, Parser, Queue, Offline-Puffer und HTTP-Weiterleitung |
| `local_dashboard.py` | lokales Streamlit-Dashboard für Kampfgericht, Import und Rundenprotokoll |
| `local_timing.py` | gemeinsamer SQLite-Speicher für Fahrer, Transponder-Mapping und Passings |
| `config.ini.example` | Vorlage für die lokale Konfiguration |
| `requirements.txt` | Python-Abhängigkeiten |
| `install_dependencies.bat` | Windows-Helfer zum Installieren der Abhängigkeiten |
| `start_bridge.bat` | Windows-Helfer zum Starten der Bridge |
| `start_dashboard.bat` | Windows-Helfer zum Starten des lokalen Dashboards |
| `mylaps_bridge.service` | Beispiel für systemd-Autostart auf Raspberry Pi/Linux |
| `sample_drivers.csv` | Beispielimport für Fahrer/Startliste |
| `sample_transponder_mapping.csv` | Beispielimport für Long-ID/Short-ID-Mapping |

Nicht ins Git gehören:

- `config.ini`
- `buffer.db`
- `local_timing.db`
- virtuelle Python-Umgebungen
- `__pycache__` und andere generierte Dateien

## Installation unter Windows

Voraussetzungen:

- Python 3.10 oder neuer
- Netzwerkzugriff vom PC zum Decoder
- optional: Netzwerkzugriff zur Radsportmanager Webapp

```powershell
cd C:\Pfad\zu\radsportmanager_decoder_bridge
copy config.ini.example config.ini
.\install_dependencies.bat
```

Danach `config.ini` anpassen:

- `decoder.host`: IP-Adresse des Decoders
- `decoder.port`: Decoder-Port, meistens `5403`
- `decoder.protocol`: `amb_p3` oder `dci`
- `server.enabled`: `yes`, wenn die Webapp angebunden werden soll
- `server.api_url`: Passing-Endpunkt der Webapp
- `server.batch_api_url`: Batch-Endpunkt der Webapp
- `server.registry_url`: Transponder-Registry der Webapp
- `server.api_key`: persönlicher API-Key aus der Webapp

Bridge starten:

```powershell
.\start_bridge.bat
```

Alternativ direkt:

```powershell
python mylaps_bridge.py
```

## Lokales Dashboard

Das lokale Dashboard läuft direkt auf dem Decoder-PC und kann auch ohne Online-Verbindung genutzt werden. Es zeigt Fahrer, Durchfahrten und Rundenstand aus `local_timing.db`.

Start:

```powershell
.\start_dashboard.bat
```

Danach im Browser öffnen:

```text
http://localhost:8501
```

Importdateien:

| Import | Pflichtspalten |
|---|---|
| Fahrer/Startliste (`.csv`, `.xlsx`, `.xls`) | `Startnummer`, `Name`, `Team`, `Short_ID` |
| Transponder-Mapping (`.csv`, `.xlsx`, `.xls`) | `Long_ID`, `Short_ID` |

Wenn die Online-Webapp nicht verwendet werden soll, kann in `config.ini` gesetzt werden:

```ini
[server]
enabled = no
```

Dann schreibt die Bridge weiterhin lokal in `local_timing.db`, sendet aber keine Daten an die Webapp.

## Installation auf Raspberry Pi / Linux

Beispiel:

```bash
cd /opt
git clone <repository-url> radsportmanager-decoder-bridge
cd radsportmanager-decoder-bridge
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp config.ini.example config.ini
python3 mylaps_bridge.py
```

Autostart mit systemd:

```bash
sudo cp mylaps_bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable mylaps_bridge
sudo systemctl start mylaps_bridge
sudo journalctl -u mylaps_bridge -f
```

Vorher in `mylaps_bridge.service` Benutzer, Arbeitsverzeichnis und Python-Pfad an die eigene Installation anpassen.

## Konfiguration

Wichtige Werte in `config.ini`:

| Parameter | Beschreibung | Beispiel |
|---|---|---|
| `decoder.host` | IP-Adresse des MYLAPS Decoders | `192.168.1.100` |
| `decoder.protocol` | Decoder-Protokoll | `amb_p3` oder `dci` |
| `decoder.port` | Decoder-Port | `5403` |
| `decoder.startup_hex` | optionale AMB-P3-Startsequenz | `020405b40103030801010402` |
| `decoder.reconnect_interval` | Sekunden bis Reconnect | `5` |
| `server.enabled` | Online-Webapp verwenden | `yes` oder `no` |
| `server.api_url` | Passing-Endpunkt der Webapp | `https://.../api/mylaps/passing.php` |
| `server.batch_api_url` | Batch-Endpunkt der Webapp | `https://.../api/mylaps/passing_batch.php` |
| `server.registry_url` | Transponder-Registry der Webapp | `https://.../api/mylaps/transponder_registry.php` |
| `server.api_key` | geheimer API-Key | `HIER_DEINEN_KEY_EINTRAGEN` |
| `server.timeout` | HTTP-Timeout in Sekunden | `5` |
| `bridge.simulate` | Simulationsmodus | `yes` oder `no` |
| `bridge.simulation_average_speed_kmh` | simulierte Durchschnittsgeschwindigkeit | `45` |
| `bridge.simulation_speed_factor` | Zeitraffer der Simulation | `10` |
| `bridge.simulation_lap_length_km` | simulierte Rundenlänge | `1.0` |
| `bridge.registry_refresh_interval` | Reload-Intervall für Registry | `60` |
| `bridge.buffer_db` | lokale Offline-Pufferdatenbank | `buffer.db` |
| `bridge.queue_max_size` | maximale interne Queue-Größe | `5000` |
| `bridge.batch_size` | maximale Batch-Größe | `100` |
| `bridge.batch_flush_interval` | Flush-Intervall für Batches | `0.5` |
| `bridge.http_workers` | parallele HTTP-Worker | `3` |
| `bridge.log_level` | Log-Level | `INFO` |
| `local.enabled` | lokales Dashboard befüllen | `yes` |
| `local.db` | lokale Dashboard-Datenbank | `local_timing.db` |

## Simulationsmodus

Für Tests ohne Decoder:

```ini
[bridge]
simulate = yes
```

Die Bridge erzeugt dann Demo-Passings und verarbeitet sie wie echte Decoder-Daten. Das ist nützlich, um lokale Installation, Dashboard, Offline-Puffer und Webapp-Anbindung zu prüfen.

## Webapp-Anbindung

Die Bridge erwartet eine Radsportmanager Webapp mit folgenden API-Funktionen:

- Transponder-Registry abrufen
- einzelne Passings empfangen
- Passings als Batch empfangen
- API-Key prüfen
- Rennen mit aktivem Timing-Modus verarbeiten

Die produktive Webapp existiert bereits, ist aber proprietär und nicht Teil dieses Repositories. Sie kann für Veranstaltungen gemietet werden. Ohne diese Webapp kann die Bridge lokal betrieben werden, aber die Online-Funktionen, Live-Anzeige und serverseitige Ergebnisverarbeitung stehen dann nicht zur Verfügung.

## Was vor einem echten Renneinsatz zu erledigen ist

1. Webapp-Zugang und API-Key einrichten.
2. `config.ini` aus `config.ini.example` erstellen.
3. Decoder-IP, Port und Protokoll prüfen.
4. Transponder-Mapping Long ID zu Short ID pflegen.
5. Fahrer- und Startlisten importieren.
6. Testlauf im Simulationsmodus durchführen.
7. Testlauf mit echtem Decoder und einigen Transpondern durchführen.
8. Offline-Puffer testen, indem die Internetverbindung kurz getrennt wird.
9. Live-Anzeige und Ergebnislogik in der Webapp prüfen.
10. API-Key geheim halten und nicht in Git einchecken.

## Bekannte offene Punkte

- AMB-P3 wurde gegen vorhandene Mitschnitte und den aktuellen Arbeitsstand getestet; weitere Decoder-Firmwarevarianten können Anpassungen erfordern.
- DCI-Unterstützung ist als Fallback vorhanden, sollte aber mit dem jeweiligen Altdecoder vor Ort getestet werden.
- Aktuell wird eine Hauptantenne bzw. ein Hauptloop verarbeitet.
- Für produktive Veranstaltungen ist ein kompletter End-to-End-Test mit Webapp, Decoder, Transpondern und Live-Anzeige erforderlich.
- Dieses Repository enthält keine öffentliche Serverinstallation der Radsportmanager Webapp.


## Rechte und Nutzung

Die Bridge ist für den Einsatz mit der Radsportmanager Webapp vorgesehen. Die Webapp, die Server-APIs und der produktive Betrieb gehören dem Projekteigentümer und können für Veranstaltungen gemietet werden.

Ohne ausdrückliche Lizenz bleiben alle Rechte vorbehalten.
