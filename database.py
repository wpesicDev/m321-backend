import aiosqlite
from datetime import datetime, timedelta

DB_PATH = "sensors.db"


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS readings (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                host      TEXT    NOT NULL,
                timestamp TEXT    NOT NULL DEFAULT (datetime('now')),
                temp      REAL,
                humi      REAL,
                airp      INTEGER,
                lum       INTEGER
            )
            """
        )
        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_readings_host_ts
            ON readings(host, timestamp)
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS hourly_readings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                host            TEXT    NOT NULL,
                hour_timestamp  TEXT    NOT NULL,
                temp            REAL,
                humi            REAL,
                airp            REAL,
                lum             REAL
            )
            """
        )
        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hourly_host_ts
            ON hourly_readings(host, hour_timestamp)
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS peers (
                url TEXT PRIMARY KEY
            )
            """
        )
        await db.commit()


async def save_reading(host: str, data: dict) -> str | None:
    """Insert a reading. Returns the timestamp on insert, None if duplicate."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO readings (host, temp, humi, airp, lum)
            VALUES (?, ?, ?, ?, ?)
            """,
            [host, data.get("temp"), data.get("humi"), data.get("airp"), data.get("lum")]
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
            INSERT OR IGNORE INTO readings (host, timestamp, temp, humi, airp, lum)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                reading.get("host"),
                reading.get("timestamp"),
                reading.get("temp"),
                reading.get("humi"),
                reading.get("airp"),
                reading.get("lum"),
            ],
        )
        await db.commit()
        return cursor.rowcount > 0


async def get_all_readings_in_range(start: str, end: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, timestamp, temp, humi, airp, lum
            FROM readings
            WHERE timestamp >= ?
              AND timestamp <= ?
            ORDER BY timestamp ASC
            """,
            [start, end],
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_all_readings() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, timestamp, temp, humi, airp, lum
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


async def aggregate_hourly():
    """Aggregate readings from the past hour and store as hourly average."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Get the hour boundary (current time rounded down to the hour)
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        hour_start_str = hour_start.isoformat(sep=' ', timespec='seconds')
        hour_end_str = now.replace(minute=0, second=0, microsecond=0).isoformat(sep=' ', timespec='seconds')
        
        # Get all unique hosts in the past hour
        async with db.execute(
            """
            SELECT DISTINCT host FROM readings
            WHERE timestamp >= ? AND timestamp < ?
            """,
            [hour_start_str, hour_end_str]
        ) as cursor:
            hosts = [row[0] for row in await cursor.fetchall()]
        
        # For each host, calculate the average of the past hour
        for host in hosts:
            async with db.execute(
                """
                SELECT
                    AVG(temp) as temp,
                    AVG(humi) as humi,
                    AVG(airp) as airp,
                    AVG(lum) as lum
                FROM readings
                WHERE host = ? AND timestamp >= ? AND timestamp < ?
                """,
                [host, hour_start_str, hour_end_str]
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0] is not None:  # Only if there's data
                    await db.execute(
                        """
                        INSERT OR REPLACE INTO hourly_readings 
                        (host, hour_timestamp, temp, humi, airp, lum)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [host, hour_start_str, row[0], row[1], row[2], row[3]]
                    )
        
        await db.commit()


async def cleanup_old_readings():
    """Delete old data: minute-level readings older than 1 hour and all data older than 7 days."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Delete minute-level readings older than 1 hour
        one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat(sep=' ', timespec='seconds')
        await db.execute(
            """
            DELETE FROM readings
            WHERE timestamp < ?
            """,
            [one_hour_ago]
        )
        
        # Delete all historical data older than 7 days
        seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat(sep=' ', timespec='seconds')
        await db.execute(
            """
            DELETE FROM hourly_readings
            WHERE hour_timestamp < ?
            """,
            [seven_days_ago]
        )
        
        await db.commit()


async def get_hourly_readings_in_range(start: str, end: str) -> list[dict]:
    """Get hourly averaged readings in a date range."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, hour_timestamp, temp, humi, airp, lum
            FROM hourly_readings
            WHERE hour_timestamp >= ?
              AND hour_timestamp <= ?
            ORDER BY hour_timestamp ASC
            """,
            [start, end],
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_hourly_readings() -> list[dict]:
    """Get all hourly averaged readings."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, hour_timestamp, temp, humi, airp, lum
            FROM hourly_readings
            ORDER BY hour_timestamp ASC
            """
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
