import aiosqlite

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
