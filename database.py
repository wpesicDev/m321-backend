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
        await db.commit()


async def save_reading(host: str, data: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO readings (host, temp, humi, airp, lum)
            VALUES (?, ?, ?, ?, ?)
            """,
            [host, data.get("temp"), data.get("humi"), data.get("airp"), data.get("lum")]
        )
        await db.commit()

async def get_latest_reading(host: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, timestamp, temp, humi, airp, lum
            FROM readings
            WHERE host = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (host,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def get_all_latest_readings() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT host, timestamp, temp, humi, airp, lum
            FROM readings
            WHERE id IN (
                SELECT MAX(id) FROM readings GROUP BY host
            )
            """
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
