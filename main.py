import asyncio
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sensor_service import HOSTS, INTERVAL, log, poll, get_current_readings, hourly_maintenance
from datetime import datetime

load_dotenv()

from database import (
    init_db,
    get_all_readings,
    get_all_readings_in_range,
    add_peer,
    remove_peer,
    get_peers,
    ingest_reading,
    get_hourly_readings_in_range,
    get_hourly_readings,
)
from sync_service import merge_with_peer, periodic_peer_sync

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
    
    # Start hourly maintenance task
    maintenance_task = asyncio.create_task(hourly_maintenance())
    log.info("started hourly maintenance task")

    yield

    log.info("stopping pollers and maintenance")
    for task in tasks:
        task.cancel()
    maintenance_task.cancel()


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


@app.get("/sensor/history")
async def sensor_history(
    start: datetime = Query(default_factory=lambda: datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)),
    end: datetime = Query(default_factory=datetime.now),
):
    if start >= end:
        raise HTTPException(400, "start must be before end")
    return await get_hourly_readings_in_range(start.isoformat(sep=' ', timespec='seconds'), end.isoformat(sep=' ', timespec='seconds'))


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
