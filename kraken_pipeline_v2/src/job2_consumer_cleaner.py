import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from confluent_kafka import Consumer, KafkaException


TOPIC = "raw_events"
BROKER = "kafka:9092"  # внутри docker-сети
GROUP_ID = "job2_cleaner_group"  # важно: фиксированный group id

SQLITE_PATH = "/opt/airflow/data/events.db"


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ingested_at TEXT NOT NULL,
        pair TEXT NOT NULL,
        kraken_symbol TEXT,
        ask_price REAL,
        bid_price REAL,
        last_price REAL,
        volume_today REAL,
        volume_24h REAL,
        vwap_today REAL,
        vwap_24h REAL,
        trades_today INTEGER,
        trades_24h INTEGER,
        low_today REAL,
        low_24h REAL,
        high_today REAL,
        high_24h REAL,
        open_price REAL,
        raw_json TEXT NOT NULL
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS daily_summary (
        day TEXT NOT NULL,
        pair TEXT NOT NULL,
        count_events INTEGER NOT NULL,
        avg_last_price REAL,
        min_last_price REAL,
        max_last_price REAL,
        avg_spread REAL,
        PRIMARY KEY (day, pair)
    )
    """)
    conn.commit()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def parse_message(msg_value: Dict[str, Any]) -> Optional[Tuple]:
    """
    msg_value формат из DAG1:
    {
      "ingested_at": "...",
      "pair": "XBTUSD",
      "raw_payload": { "error": [], "result": { "XXBTZUSD": {...} } }
    }
    """
    ingested_at = msg_value.get("ingested_at") or datetime.now(timezone.utc).isoformat()
    pair = msg_value.get("pair")

    raw_payload = msg_value.get("raw_payload", {})
    result = (raw_payload or {}).get("result") or {}
    if not pair or not isinstance(result, dict) or len(result) == 0:
        return None

    # Берём первый ключ (например "XXBTZUSD")
    kraken_symbol = next(iter(result.keys()))
    ticker = result.get(kraken_symbol, {}) or {}

    # Kraken поля (массивы строк)
    # a: [ask, whole lot volume, lot volume]
    # b: [bid, ...]
    # c: [last trade closed price, lot volume]
    ask_price = _safe_float((ticker.get("a") or [None])[0])
    bid_price = _safe_float((ticker.get("b") or [None])[0])
    last_price = _safe_float((ticker.get("c") or [None])[0])

    volume_today = _safe_float((ticker.get("v") or [None, None])[0])
    volume_24h = _safe_float((ticker.get("v") or [None, None])[1])

    vwap_today = _safe_float((ticker.get("p") or [None, None])[0])
    vwap_24h = _safe_float((ticker.get("p") or [None, None])[1])

    trades_today = _safe_int((ticker.get("t") or [None, None])[0])
    trades_24h = _safe_int((ticker.get("t") or [None, None])[1])

    low_today = _safe_float((ticker.get("l") or [None, None])[0])
    low_24h = _safe_float((ticker.get("l") or [None, None])[1])

    high_today = _safe_float((ticker.get("h") or [None, None])[0])
    high_24h = _safe_float((ticker.get("h") or [None, None])[1])

    open_price = _safe_float(ticker.get("o"))

    # Фильтр "валидности" (пример)
    if last_price is None:
        return None

    raw_json = json.dumps(msg_value, ensure_ascii=False)

    return (
        ingested_at, pair, kraken_symbol,
        ask_price, bid_price, last_price,
        volume_today, volume_24h,
        vwap_today, vwap_24h,
        trades_today, trades_24h,
        low_today, low_24h,
        high_today, high_24h,
        open_price,
        raw_json
    )


def write_event(conn: sqlite3.Connection, row: Tuple) -> None:
    conn.execute("""
    INSERT INTO events (
        ingested_at, pair, kraken_symbol,
        ask_price, bid_price, last_price,
        volume_today, volume_24h,
        vwap_today, vwap_24h,
        trades_today, trades_24h,
        low_today, low_24h,
        high_today, high_24h,
        open_price,
        raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)


def main(max_messages: int = 500, max_seconds: int = 55) -> None:
    """
    Читает новые сообщения из Kafka и пишет в SQLite.
    max_seconds держим < 60, чтобы таска не висела долго.
    """
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",   # первый запуск заберёт историю
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    conn = sqlite3.connect(SQLITE_PATH)
    ensure_tables(conn)

    start = datetime.now(timezone.utc)
    processed = 0

    try:
        while processed < max_messages:
            # выходим по времени
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            if elapsed > max_seconds:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                val = json.loads(msg.value().decode("utf-8"))
            except Exception:
                # пропускаем мусор
                consumer.commit(message=msg, asynchronous=False)
                continue

            row = parse_message(val)
            if row is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            write_event(conn, row)
            conn.commit()

            # фиксируем offset только после успешной записи
            consumer.commit(message=msg, asynchronous=False)
            processed += 1

    finally:
        conn.close()
        consumer.close()

def main():
    conn = sqlite3.connect(SQLITE_PATH)
    ensure_tables(conn)

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # 🔥 важно
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())

            value = json.loads(msg.value().decode("utf-8"))
            row = parse_message(value)
            if row is None:
                continue

            conn.execute("""
                INSERT INTO events (
                    ingested_at, pair, kraken_symbol,
                    ask_price, bid_price, last_price,
                    volume_today, volume_24h,
                    vwap_today, vwap_24h,
                    trades_today, trades_24h,
                    low_today, low_24h,
                    high_today, high_24h,
                    open_price, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)

            conn.commit()

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()

