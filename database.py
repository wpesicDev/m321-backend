import logging
from collections import defaultdict
from datetime import datetime, timedelta

import aiosqlite

log = logging.getLogger("database")

DB_PATH = "sensors.db"

GRANULARITY_15S = "15s"
GRANULARITY_5M = "5m"
GRANULARITY_30M = "30m"
GRANULARITY_3H = "3h"

# Heute / <7 Tage: 15s | 8–7 Tage: 5m | 45–44 Tage: 30m | 100–99 Tage: 3h
RETENTION_15S = timedelta(days=7)   # älter → 15s wird zu 5m
RETENTION_5M = timedelta(days=44)   # älter → 5m wird zu 30m
RETENTION_30M = timedelta(days=99)  # älter → 30m wird zu 3h

BUCKET_5M_SECONDS = 300   # 20 × 15s
BUCKET_30M_SECONDS = 1800  # 6 × 5m
BUCKET_3H_SECONDS = 10800  # 6 × 30m


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS readings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                host        TEXT    NOT NULL,
                timestamp   TEXT    NOT NULL DEFAULT (datetime('now')),
                granularity TEXT    NOT NULL DEFAULT '15s',
                temp        REAL,
                humi        REAL,
                airp        INTEGER,
                lum         INTEGER
            )
            """
        )
        await _migrate_add_granularity(db)
        await _dedupe_readings(db)
        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_readings_host_ts
            ON readings(host, timestamp)
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_readings_gran_ts ON readings(granularity, timestamp)"
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS peers (
                url TEXT PRIMARY KEY
            )
            """
        )
        await db.commit()


async def _migrate_add_granularity(db: aiosqlite.Connection):
    async with db.execute("PRAGMA table_info(readings)") as cursor:
        rows = await cursor.fetchall()
        columns = {row[1] for row in rows}
    if "granularity" not in columns:
        await db.execute(
            "ALTER TABLE readings ADD COLUMN granularity TEXT NOT NULL DEFAULT '15s'"
        )


async def _dedupe_readings(db: aiosqlite.Connection):
    """Entfernt Duplikate (host, timestamp), damit der Unique-Index angelegt werden kann."""
    await db.execute(
        """
        DELETE FROM readings
        WHERE id NOT IN (
            SELECT MIN(id) FROM readings GROUP BY host, timestamp
        )
        """
    )


async def save_reading(host: str, data: dict) -> str | None:
    """Insert a reading. Returns the timestamp on insert, None if duplicate."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO readings (host, granularity, temp, humi, airp, lum)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                host,
                GRANULARITY_15S,
                data.get("temp"),
                data.get("humi"),
                data.get("airp"),
                data.get("lum"),
            ],
        )
        if cursor.rowcount == 0:
            await db.commit()
            return None
        async with db.execute(
            "SELECT timestamp FROM readings WHERE id = ?", [cursor.lastrowid]
        ) as c:
            row = await c.fetchone()
        await db.commit()
        return row[0] if row else None


async def ingest_reading(reading: dict) -> bool:
    """Insert a reading received from a peer. Returns True if newly inserted."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO readings (host, timestamp, granularity, temp, humi, airp, lum)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                reading.get("host"),
                reading.get("timestamp"),
                reading.get("granularity", GRANULARITY_15S),
                reading.get("temp"),
                reading.get("humi"),
                reading.get("airp"),
                reading.get("lum"),
            ],
        )
        await db.commit()
        return cursor.rowcount > 0


async def get_readings_in_range(start: str, end: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, timestamp, granularity, temp, humi, airp, lum
            FROM readings
            WHERE timestamp >= ?
              AND timestamp < ?
            ORDER BY timestamp ASC
            """,
            [start, end],
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_all_readings_in_range(start: str, end: str) -> list[dict]:
    return await get_readings_in_range(start, end)


async def get_all_readings() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, timestamp, granularity, temp, humi, airp, lum
            FROM readings
            ORDER BY timestamp ASC
            """
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def add_peer(url: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO peers (url) VALUES (?)", [url])
        await db.commit()


async def remove_peer(url: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("DELETE FROM peers WHERE url = ?", [url])
        await db.commit()
        return cursor.rowcount > 0


async def get_peers() -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT url FROM peers") as cursor:
            return [row[0] for row in await cursor.fetchall()]


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _format_ts(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat(sep=" ", timespec="seconds")


def _bucket_start(ts: str, bucket_seconds: int) -> str:
    dt = _parse_ts(ts)
    epoch = int(dt.timestamp())
    bucket_epoch = epoch - (epoch % bucket_seconds)
    return _format_ts(datetime.fromtimestamp(bucket_epoch))


def _avg(values: list) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


async def _aggregate_step(
    source_granularity: str,
    target_granularity: str,
    bucket_seconds: int,
    older_than: timedelta,
) -> int:
    cutoff = _format_ts(datetime.now() - older_than)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, host, timestamp, temp, humi, airp, lum
            FROM readings
            WHERE granularity = ?
              AND timestamp < ?
            ORDER BY timestamp ASC
            """,
            [source_granularity, cutoff],
        ) as cursor:
            raw_rows = await cursor.fetchall()
            rows = [dict(row) for row in raw_rows]

        if not rows:
            return 0

        buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for row in rows:
            bucket_ts = _bucket_start(row["timestamp"], bucket_seconds)
            buckets[(row["host"], bucket_ts)].append(row)

        aggregated = []
        ids_to_delete = []
        for (host, bucket_ts), group in buckets.items():
            aggregated.append(
                (
                    host,
                    bucket_ts,
                    target_granularity,
                    _avg([r["temp"] for r in group]),
                    _avg([r["humi"] for r in group]),
                    _avg([r["airp"] for r in group]),
                    _avg([r["lum"] for r in group]),
                )
            )
            ids_to_delete.extend(r["id"] for r in group)

        await db.executemany(
            """
            INSERT OR IGNORE INTO readings (host, timestamp, granularity, temp, humi, airp, lum)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            aggregated,
        )
        placeholders = ",".join("?" * len(ids_to_delete))
        await db.execute(
            f"DELETE FROM readings WHERE id IN ({placeholders})",
            ids_to_delete,
        )
        await db.commit()
        return len(aggregated)


async def run_retention_aggregation() -> dict[str, int]:
    """15s→5m (>7d), 5m→30m (>44d), 30m→3h (>99d). Ziel: Heute 15s, 8–7d 5m, 45–44d 30m, 100–99d 3h."""
    counts = {
        "5m": await _aggregate_step(
            GRANULARITY_15S, GRANULARITY_5M, BUCKET_5M_SECONDS, RETENTION_15S
        ),
        "30m": await _aggregate_step(
            GRANULARITY_5M, GRANULARITY_30M, BUCKET_30M_SECONDS, RETENTION_5M
        ),
        "3h": await _aggregate_step(
            GRANULARITY_30M, GRANULARITY_3H, BUCKET_3H_SECONDS, RETENTION_30M
        ),
    }
    return counts
