"""Lokales Testdaten-Skript – nicht für Produktion. Schreibt nur in sensors.db."""
import asyncio
import math
from datetime import datetime, timedelta

import aiosqlite

from database import DB_PATH, GRANULARITY_15S, init_db, run_retention_aggregation

HOST = "10.125.218.57"
INTERVAL_SECONDS = 15


def _ts(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat(sep=" ", timespec="seconds")


async def _insert_range(start: datetime, end: datetime):
    async with aiosqlite.connect(DB_PATH) as db:
        current = start
        i = 0
        while current < end:
            temp = 20.0 + 3.0 * math.sin(i / 40)
            humi = 50.0 + 5.0 * math.cos(i / 30)
            airp = 101300 + (i % 10)
            lum = 100 + (i % 50)
            await db.execute(
                """
                INSERT OR IGNORE INTO readings
                    (host, timestamp, granularity, temp, humi, airp, lum)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (HOST, _ts(current), GRANULARITY_15S, temp, humi, airp, lum),
            )
            current += timedelta(seconds=INTERVAL_SECONDS)
            i += 1
        await db.commit()


async def main():
    await init_db()
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    ranges = [
        ("Heute (bleibt 15s)", today_start, now),
        ("8–7 Tage (→5m)", now - timedelta(days=8), now - timedelta(days=7)),
        ("45–44 Tage (→5m→30m)", now - timedelta(days=45), now - timedelta(days=44)),
        ("100–99 Tage (→5m→30m→3h)", now - timedelta(days=100), now - timedelta(days=99)),
    ]

    for label, start, end in ranges:
        await _insert_range(start, end)
        count = int((end - start).total_seconds() / INTERVAL_SECONDS)
        print(f"inserted ~{count} rows: {label}")

    summary = await run_retention_aggregation()
    print("aggregation:", summary)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT granularity, COUNT(*) FROM readings GROUP BY granularity ORDER BY granularity"
        ) as cur:
            rows = await cur.fetchall()
        print("counts by granularity:", dict(rows))


if __name__ == "__main__":
    asyncio.run(main())
