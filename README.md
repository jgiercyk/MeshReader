# Mesh Command Post

A read-only Meshtastic MQTT monitor for Windows. Connects to a Meshtastic MQTT broker, subscribes to JSON-format packet topics, displays live packets, tracks heard nodes, and provides a simple dashboard of mesh activity.

---

## Important: Read-Only Design

**Mesh Command Post does not transmit anything to the mesh.** It only subscribes to MQTT topics and displays what it receives. No messages, no downlinks, no configuration packets are sent. This is intentional.

---

## What the App Does

| Tab | Purpose |
|-----|---------|
| **Packets** | Live feed of all received MQTT packets with filtering, pause/resume, and JSON detail view |
| **Nodes** | Table of every node heard, updated from `nodeinfo`, `position`, and `mapreport` packets |
| **Map** | Leaflet/OpenStreetMap map showing nodes with GPS coordinates |
| **Messages** | Text/message packets only |
| **Telemetry** | Device and environment metrics in a structured table |

All packets and node records are saved to a local SQLite database (`~/.mesh_command_post/history.db`) and loaded automatically on next launch.

---

## Public MQTT Limitations

The default broker (`mqtt.meshtastic.org`) is a community relay. Not every real-world node appears there — nodes must have MQTT uplink enabled in their firmware settings. Expect to see a subset of active mesh nodes.

---

## Requirements

- Python 3.12 or newer
- Windows 10/11 (tested), Linux/macOS should work but are not the target
- Internet access for the Map tab (OpenStreetMap tiles loaded from CDN)

---

## Install Dependencies

```powershell
pip install -r requirements.txt
```

To verify PySide6 WebEngine is available (needed for the Map tab):

```python
from PySide6.QtWebEngineWidgets import QWebEngineView
```

If that import fails, reinstall PySide6:

```powershell
pip install --force-reinstall PySide6
```

---

## Run from Source

```powershell
cd mesh-command-post\src
python main.py
```

Or from the project root:

```powershell
python mesh-command-post\src\main.py
```

---

## Connect to the Mesh

1. Launch the app.
2. The default settings point to the public Meshtastic broker — no changes needed for a first test.
3. Click **Connect**.
4. The status indicator turns green and shows the subscribed topic.
5. Within seconds (if any nodes are active on the network), packets begin appearing in the Packets tab.
6. Nodes with `nodeinfo` or `position` packets populate the Nodes and Map tabs automatically.

Default connection settings:

| Setting | Value |
|---------|-------|
| Broker | `mqtt.meshtastic.org` |
| Port | `1883` |
| TLS | Off |
| Username | `meshdev` |
| Password | `large4cats` |
| Topic | `msh/US/2/json/#` |

All settings are editable in the Connection panel and saved to `~/.mesh_command_post/settings.json` on exit.

---

## Build the Windows Executable

```powershell
cd mesh-command-post
.\build.ps1
```

The standalone `.exe` is written to `dist\MeshCommandPost.exe`. First build takes a few minutes.

To clean artifacts before rebuilding:

```powershell
.\build.ps1 -Clean
```

> **Note:** The built `.exe` is self-contained but still requires internet access to load OpenStreetMap tiles in the Map tab. The MQTT connection and all other tabs work fully offline.

---

## Export

- **Packets tab** → "Export CSV" or "Export JSONL" button in the filter bar
- **Nodes tab** → "Export CSV" button
- **Event log area** → "Export DB JSONL" exports the full SQLite packet history as newline-delimited JSON

---

## Data Storage

| File | Contents |
|------|---------|
| `~/.mesh_command_post/history.db` | SQLite: all packets + node table |
| `~/.mesh_command_post/settings.json` | MQTT connection settings |

---

## Future Feature Backlog (not yet built)

- Protobuf decode support for `msh/REGION/2/e/#` topics
- Private MQTT broker mode
- Callsign / node watchlist with alerts
- Distance from a home grid or coordinate
- Alerts for nodes heard near a specific location
- MQTT replay from saved JSONL
- KML export
- Integration with ham radio command dashboards
