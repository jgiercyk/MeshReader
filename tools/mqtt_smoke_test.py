"""
Standalone MQTT smoke test.

Imports: ONLY Python standard library + paho.mqtt.client
Does NOT import: app, main, storage, source_manager, or any project module
Does NOT open: SQLite, GUI, config files
Does NOT use: Root Manager, subscription registry

Usage (from any directory):
    python tools/mqtt_smoke_test.py

Or override topic/duration:
    python tools/mqtt_smoke_test.py msh/US/2/json/# 30
"""
# ── stdlib only ───────────────────────────────────────────────────────────────
import os
import random
import sys
import time
import traceback

# Strip project src/ from path so we cannot accidentally import app modules
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)
_src_dir = os.path.join(_project_root, "src")
sys.path = [p for p in sys.path if os.path.abspath(p) != _src_dir]

# ── single third-party import ─────────────────────────────────────────────────
try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt not installed.  pip install paho-mqtt")
    sys.exit(1)

# ── config — edit here if needed ─────────────────────────────────────────────
BROKER   = "mqtt.meshtastic.org"
PORT     = 1883
USERNAME = "meshdev"
PASSWORD = "large4cats"
TOPIC    = sys.argv[1] if len(sys.argv) > 1 else "msh/US/2/map/#"
DURATION = int(sys.argv[2]) if len(sys.argv) > 2 else 60

CLIENT_ID = f"mcp_smoke_{int(time.time())}_{random.randint(1000, 9999)}"

# ── state ─────────────────────────────────────────────────────────────────────
_received   = 0
_start_time = None

# ── callbacks ─────────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    global _start_time
    labels = {0: "OK", 1: "Bad protocol", 2: "Bad client ID",
              3: "Broker unavailable", 4: "Bad credentials", 5: "Not authorized"}
    print(f"[CONNECT]    rc={rc}  {labels.get(rc, 'unknown')}")
    if rc == 0:
        result, mid = client.subscribe(TOPIC, qos=0)
        print(f"[SUBSCRIBE]  topic={TOPIC!r}  result={result}  mid={mid}")
        _start_time = time.monotonic()
    else:
        print("[CONNECT]    Cannot continue — disconnecting.")
        client.disconnect()


def on_subscribe(client, userdata, mid, granted_qos):
    print(f"[SUBACK]     mid={mid}  granted_qos={granted_qos}")


def on_message(client, userdata, msg):
    global _received
    _received += 1
    elapsed = time.monotonic() - _start_time if _start_time else 0.0
    print(f"[PKT #{_received:4d}]  t={elapsed:6.1f}s  "
          f"len={len(msg.payload):4d}  topic={msg.topic}")


def on_disconnect(client, userdata, rc):
    labels = {
        0: "clean",  6: "not found / keepalive timeout",
        7: "connection lost",  16: "keepalive timeout",
    }
    print(f"[DISCONNECT] rc={rc}  {labels.get(rc, f'paho rc={rc}')}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  MQTT Smoke Test — standalone, no app code")
    print(f"  broker   : {BROKER}:{PORT}")
    print(f"  topic    : {TOPIC}")
    print(f"  client ID: {CLIENT_ID}")
    print(f"  user     : {USERNAME}")
    print(f"  duration : {DURATION}s")
    print("=" * 60)

    try:
        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                client_id=CLIENT_ID,
            )
        except (AttributeError, TypeError):
            client = mqtt.Client(client_id=CLIENT_ID)

        client.username_pw_set(USERNAME, PASSWORD)
        client.on_connect    = on_connect
        client.on_subscribe  = on_subscribe
        client.on_message    = on_message
        client.on_disconnect = on_disconnect

        print(f"[CONNECTING] {BROKER}:{PORT} …")
        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()

        # Wait for subscribe + duration window
        for _ in range((DURATION + 10) * 4):   # 0.25s ticks
            time.sleep(0.25)
            if _start_time and (time.monotonic() - _start_time) >= DURATION:
                break

        client.loop_stop()
        client.disconnect()
        time.sleep(0.5)

    except Exception:
        print("\n[EXCEPTION]")
        traceback.print_exc()
        sys.exit(1)

    print("=" * 60)
    print(f"  Received {_received} packet(s) over {DURATION}s")
    print("=" * 60)

    if _received == 0:
        print("\nRESULT: Zero packets.")
        print("  rc=0 at connect → broker accepted creds; topic may be quiet or wrong case.")
        print("  rc≠0 at connect → bad credentials / network / firewall.")
    else:
        print("\nRESULT: PASS — broker OK, credentials OK, packets flowing.")
        print("  Any subscription issues are in the GUI layer.")


if __name__ == "__main__":
    main()
