# producer/opensky_producer.py
# Polls OpenSky /states/all over a bounding box and emits one JSON event per
# aircraft. Runs OUTSIDE Databricks (local machine or VM), like a real sensor
# feed. Sink is Event Hub for prod or local JSON Lines files for dev.
#
# Secrets come from .env (never committed): OPENSKY_CLIENT_ID,
# OPENSKY_CLIENT_SECRET, and for eventhub mode EH_CONN_STR, EH_NAME.

import os, json, time, logging
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL  = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"
BBOX = {"lamin": 49.0, "lomin": 14.1, "lamax": 54.9, "lomax": 24.2}   # Poland

# OpenSky returns each aircraft as a positional array, keep only the 17 documented fields
FIELDS = ["icao24", "callsign", "origin_country", "time_position", "last_contact",
          "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
          "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
          "spi", "position_source"]

SINK     = os.environ.get("SINK", "file")          # "file" | "eventhub"
INTERVAL = int(os.environ.get("INTERVAL", "30"))
OUT_DIR  = os.environ.get("OUT_DIR", "landing")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class TokenManager:
    """OAuth2 client-credentials tokens expire after ~30 min; refresh on demand."""
    def __init__(self):
        self.token, self.expires_at = None, 0

    def get(self):
        if time.time() > self.expires_at - 60:
            r = requests.post(TOKEN_URL, data={
                "grant_type": "client_credentials",
                "client_id": os.environ["OPENSKY_CLIENT_ID"],
                "client_secret": os.environ["OPENSKY_CLIENT_SECRET"]})
            r.raise_for_status()
            body = r.json()
            self.token = body["access_token"]
            self.expires_at = time.time() + body.get("expires_in", 1800)
            logging.info("token refreshed")
        return self.token


def fetch_states(tm):
    r = requests.get(STATES_URL, params=BBOX,
                     headers={"Authorization": f"Bearer {tm.get()}"}, timeout=20)
    r.raise_for_status()
    body = r.json()
    snapshot_ts = datetime.fromtimestamp(body["time"], tz=timezone.utc).isoformat()
    # Positional array -> named dict, keep only the 17 documented fields
    return [dict(zip(FIELDS, s[:17]), snapshot_ts=snapshot_ts)
            for s in (body.get("states") or [])]


def sink_file(events):
    os.makedirs(OUT_DIR, exist_ok=True)
    name = f"states_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
    with open(os.path.join(OUT_DIR, name), "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return name


def sink_eventhub(producer, events):
    from azure.eventhub import EventData
    batch = producer.create_batch()
    for e in events:
        batch.add(EventData(json.dumps(e)))
    producer.send_batch(batch)


def main():
    tm = TokenManager()
    producer = None
    if SINK == "eventhub":
        from azure.eventhub import EventHubProducerClient
        producer = EventHubProducerClient.from_connection_string(
            os.environ["EH_CONN_STR"], eventhub_name=os.environ["EH_NAME"])

    logging.info("producer started | sink=%s interval=%ss", SINK, INTERVAL)
    while True:
        try:
            events = fetch_states(tm)
            if SINK == "eventhub":
                sink_eventhub(producer, events)
                logging.info("sent %d events", len(events))
            else:
                logging.info("wrote %d events to %s", len(events), sink_file(events))
        except Exception as e:
            logging.warning("cycle failed: %s", e)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()