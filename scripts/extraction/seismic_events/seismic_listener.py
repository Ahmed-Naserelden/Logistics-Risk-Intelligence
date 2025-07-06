from __future__ import unicode_literals

from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop
from tornado import gen
import logging
import json
import sys
import psycopg2

# WebSocket endpoint
SEISMIC_WS_URI = 'wss://www.seismicportal.eu/standing_order/websocket'
PING_INTERVAL = 15

# PostgreSQL connection settings (inside Docker network)
DB_SETTINGS = {
    'host': 'localhost',       # Connecting from outside Docker
    'port': 7432,              # Because you exposed 7432:5432
    'database': 'maritime_logistics',
    'user': 'postgres',
    'password': 'postgres'
}


# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL database.")
    logging.info("✅ Connected to PostgreSQL database.")
except Exception:
    logging.exception("❌ Could not connect to PostgreSQL.")
    sys.exit(1)

# Create table if not exists
def create_table():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS seismic_events (
        unid TEXT PRIMARY KEY,
        source_id TEXT,
        source_catalog TEXT,
        lastupdate TIMESTAMP,
        time TIMESTAMP,
        flynn_region TEXT,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        depth DOUBLE PRECISION,
        evtype TEXT,
        auth TEXT,
        mag DOUBLE PRECISION,
        magtype TEXT,
        action TEXT,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()

create_table()

# Handle and insert/update/delete data
def process_event(message):
    print(f"🔍 Processing message: {message}")
    try:
        data = json.loads(message)
        props = data['data']['properties']
        action = data['action']
        unid = props.get('unid')

        if not unid:
            logging.warning("⚠️ Missing UNID, skipping message.")
            return

        if action == "UPDATE":
            cursor.execute("""
                UPDATE seismic_events SET
                    source_id = %s, source_catalog = %s, lastupdate = %s, time = %s,
                    flynn_region = %s, lat = %s, lon = %s, depth = %s,
                    evtype = %s, auth = %s, mag = %s, magtype = %s, action = %s,
                    received_at = CURRENT_TIMESTAMP
                WHERE unid = %s
            """, (
                props.get('source_id'), props.get('source_catalog'),
                props.get('lastupdate'), props.get('time'), props.get('flynn_region'),
                props.get('lat'), props.get('lon'), props.get('depth'),
                props.get('evtype'), props.get('auth'), props.get('mag'),
                props.get('magtype'), action, unid
            ))

        elif action == "DELETE":
            cursor.execute("DELETE FROM seismic_events WHERE unid = %s", (unid,))

        else:  # INSERT or CREATE
            cursor.execute("""
                INSERT INTO seismic_events (
                    unid, source_id, source_catalog, lastupdate, time,
                    flynn_region, lat, lon, depth, evtype, auth, mag,
                    magtype, action
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (unid) DO NOTHING
            """, (
                unid, props.get('source_id'), props.get('source_catalog'),
                props.get('lastupdate'), props.get('time'), props.get('flynn_region'),
                props.get('lat'), props.get('lon'), props.get('depth'),
                props.get('evtype'), props.get('auth'), props.get('mag'),
                props.get('magtype'), action
            ))

        conn.commit()
        logging.info(f"✅ Processed {action} for UNID={unid}")

    except Exception:
        logging.exception("❌ Failed to process/insert message.")

# Read WebSocket stream
@gen.coroutine
def listen(ws):
    while True:
        msg = yield ws.read_message()
        if msg is None:
            logging.info("🔌 WebSocket closed")
            break
        process_event(msg)

# Connect to WebSocket
@gen.coroutine
def launch_client():
    try:
        logging.info(f"🌐 Connecting to WebSocket: {SEISMIC_WS_URI}")
        ws = yield websocket_connect(SEISMIC_WS_URI, ping_interval=PING_INTERVAL)
    except Exception:
        logging.exception("❌ WebSocket connection failed")
    else:
        logging.info("🟢 Listening for real-time seismic data...")
        yield listen(ws)

# Start the loop
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    loop = IOLoop.instance()
    launch_client()
    try:
        loop.start()
    except KeyboardInterrupt:
        logging.info("🛑 Keyboard interrupt. Shutting down.")
        loop.stop()
        cursor.close()
        conn.close()
