import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

import cluster
from cluster import (
    NODE_ID,
    NODE_URL,
    broadcast_loop,
    listen_loop,
    state as cluster_state,
)
from tcp import HOSTS, INTERVAL, cache, log, poll

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    log.info("node %d starting at %s, polling %s every %.1fs", NODE_ID, NODE_URL, HOSTS, INTERVAL)
    bg = [
        asyncio.create_task(broadcast_loop()),
        asyncio.create_task(listen_loop()),
        *[asyncio.create_task(poll(host)) for host in HOSTS],
    ]
    yield
    for task in bg:
        task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {
        "node": NODE_ID,
        "url": NODE_URL,
        "is_leader": cluster_state["is_leader"],
        "peers": cluster.alive_peers(),
        "hosts": HOSTS,
        "interval": INTERVAL,
    }


@app.get("/sensor")
async def all_sensors():
    return cache


@app.get("/sensor/{host}")
async def one_sensor(host: str):
    if host not in cache:
        raise HTTPException(404, "host not polled yet")
    return cache[host]
