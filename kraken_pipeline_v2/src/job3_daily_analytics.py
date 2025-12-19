import sqlite3
from datetime import datetime, timezone, date

SQLITE_PATH = "/opt/airflow/data/events.db"


def ensure_table(conn: sqlite3.Connection) -> None:
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


def main(target_day: str | None = None) -> None:
    """
    target_day: 'YYYY-MM-DD' (UTC). Если None — берём сегодняшний день (UTC).
    """
    if target_day is None:
        target_day = date.today().isoformat()

    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()
    ensure_table(conn)

    # агрегаты за день (UTC)
    cur.execute(
        """
        INSERT OR REPLACE INTO daily_summary (
            day, pair, count_events,
            avg_last_price, min_last_price, max_last_price, avg_spread
        )
        SELECT
            date(ingested_at) AS day,
            pair,
            COUNT(*) AS count_events,
            AVG(last_price) AS avg_last_price,
            MIN(last_price) AS min_last_price,
            MAX(last_price) AS max_last_price,
            AVG(ask_price - bid_price) AS avg_spread
        FROM events
        WHERE date(ingested_at) = ?
        GROUP BY day, pair
        """,
        (target_day,),
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()

