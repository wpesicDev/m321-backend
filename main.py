import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from tcp import HOSTS, INTERVAL, log, poll
from database import init_db, get_latest_reading, get_all_latest_readings

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


@app.get("/sensor")
async def all_sensors():
    return await get_all_latest_readings()


@app.get("/sensor/{host}")
async def one_sensor(host: str):
    data = await get_latest_reading(host)
    if data is None:
        raise HTTPException(404, "no data for this host")
    return data


