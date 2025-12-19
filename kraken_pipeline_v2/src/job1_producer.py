import json
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

KRAKEN_URL = "https://api.kraken.com/0/public/Ticker"
PAIR = "XBTUSD"          # можешь поменять
TOPIC = "raw_events"
BROKER = "kafka:9092"    # ВАЖНО: внутри docker-сети


def fetch_ticker(pair: str) -> dict:
    r = requests.get(KRAKEN_URL, params={"pair": pair}, timeout=10)
    r.raise_for_status()
    return r.json()


def main():
    p = Producer({"bootstrap.servers": BROKER})

    payload = fetch_ticker(PAIR)

    msg = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "pair": PAIR,
        "raw_payload": payload,
    }

    p.produce(TOPIC, value=json.dumps(msg).encode("utf-8"))
    p.flush(10)


if __name__ == "__main__":
    main()

