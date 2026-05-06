import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from tcp import HOSTS, INTERVAL, log, poll
from datetime import datetime

from database import init_db, get_latest_reading, get_all_latest_readings, get_all_readings_in_range

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    log.info("starting pollers for %s every %.1fs", HOSTS, INTERVAL)
    tasks = [asyncio.create_task(poll(host)) for host in HOSTS]

    yield

    log.info("stopping pollers")
    for task in tasks:
        task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"hosts": HOSTS, "interval": INTERVAL}


@app.get("/sensor/history")
async def sensor_history(
    start: datetime = Query(default_factory=lambda: datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)),
    end: datetime = Query(default_factory=datetime.now),
):
    if start >= end:
        raise HTTPException(400, "start must be before end")
    return await get_all_readings_in_range(start.isoformat(sep=' ', timespec='seconds'), end.isoformat(sep=' ', timespec='seconds'))


