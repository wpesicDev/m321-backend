import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from tcp import HOSTS, INTERVAL, cache, log, poll

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(_: FastAPI):
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
    return cache


@app.get("/sensor/{host}")
async def one_sensor(host: str):
    if host not in cache:
        raise HTTPException(404, "host not polled yet")
    return cache[host]
