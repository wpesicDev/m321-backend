import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aggregation import daily_aggregation_loop
from database import (
    add_peer,
    get_all_readings,
    get_peers,
    get_readings_in_range,
    init_db,
    ingest_reading,
    remove_peer,
    run_retention_aggregation,
)
from sensor_service import HOSTS, INTERVAL, log, poll, get_current_readings
from tcp import query, parse_response
from sync_service import merge_with_peer, periodic_peer_sync

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()

    peer_urls = [u.strip() for u in os.getenv("PEER_URLS", "").split(",") if u.strip()]
    for url in peer_urls:
        await add_peer(url)
    if peer_urls:
        log.info("registered peers from env: %s", peer_urls)

    log.info("starting pollers for %s every %.1fs", HOSTS, INTERVAL)
    tasks = [asyncio.create_task(poll(host)) for host in HOSTS]
    tasks.append(asyncio.create_task(periodic_peer_sync()))
    tasks.append(asyncio.create_task(daily_aggregation_loop()))

    yield

    log.info("stopping pollers, sync and aggregation")
    for task in tasks:
        task.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class PeerRequest(BaseModel):
    url: str


@app.get("/sensor/current")
async def sensor_current():
    return await get_current_readings()


@app.get("/sensor/image")
async def sensor_image():
    for host in HOSTS:
        try:
            response = await query(host, "img")
            result = parse_response(response, ["img"])
            if "error" not in result:
                return {"host": host, "img": result["img"]}
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            continue
    raise HTTPException(503, "no sensor host returned an image")


@app.get("/sensor/beep")
async def sensor_beep():
    for host in HOSTS:
        try:
            response = await query(host, "beep")
            result = parse_response(response, ["beep"])
            if "error" not in result:
                return {"host": host, "beep": result["beep"]}
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            continue
    raise HTTPException(503, "no sensor host responded to beep")


@app.get("/sensor/history")
async def sensor_history(
    day: date | None = Query(
        None,
        description="Single day (YYYY-MM-DD). Returns all stored values for this day.",
        examples=["2026-05-20"],
    ),
    start: datetime | None = Query(
        None,
        description="Startzeit (nur wenn day nicht gesetzt). Standard: heute 00:00.",
    ),
    end: datetime | None = Query(
        None,
        description="Endzeit (nur wenn day nicht gesetzt). Standard: jetzt.",
    ),
):
    if day is not None:
        start_dt = datetime.combine(day, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)
    else:
        start_dt = start or datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_dt = end or datetime.now()

    if start_dt >= end_dt:
        raise HTTPException(400, "start must be before end")

    readings = await get_readings_in_range(
        start_dt.isoformat(sep=" ", timespec="seconds"),
        end_dt.isoformat(sep=" ", timespec="seconds"),
    )
    return {
        "day": day.isoformat() if day else None,
        "start": start_dt.isoformat(sep=" ", timespec="seconds"),
        "end": end_dt.isoformat(sep=" ", timespec="seconds"),
        "count": len(readings),
        "readings": readings,
    }


@app.post("/sensor/aggregate")
async def sensor_aggregate():
    """Manual trigger for retention aggregation (e.g., via cron)."""
    summary = await run_retention_aggregation()
    return {"aggregated_buckets": summary}


@app.post("/sync/peer")
async def sync_peer(body: PeerRequest):
    """Register a peer instance and perform an initial bidirectional merge."""
    await add_peer(body.url)
    try:
        result = await merge_with_peer(body.url)
    except Exception as e:
        raise HTTPException(502, f"merge with peer failed: {e}")
    return {"peer": body.url, **result}


@app.delete("/sync/peer")
async def sync_peer_remove(body: PeerRequest):
    removed = await remove_peer(body.url)
    if not removed:
        raise HTTPException(404, "peer not registered")
    return {"removed": body.url}


@app.get("/sync/peers")
async def sync_peers_list():
    return await get_peers()


@app.get("/sync/dump")
async def sync_dump():
    return await get_all_readings()


@app.post("/sync/ingest")
async def sync_ingest(readings: list[dict]):
    inserted = 0
    for reading in readings:
        if await ingest_reading(reading):
            inserted += 1
    return {"received": len(readings), "inserted": inserted}
