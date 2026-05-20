import asyncio
import logging

import httpx

from database import get_all_readings, get_peers, ingest_reading

log = logging.getLogger("sync")
TIMEOUT = 10.0


async def broadcast_reading(reading: dict):
    """Push a freshly saved reading to all registered peers. Fire-and-forget per peer."""
    peers = await get_peers()
    if not peers:
        return
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        await asyncio.gather(
            *(_push_one(client, peer, reading) for peer in peers),
            return_exceptions=True,
        )


async def _push_one(client: httpx.AsyncClient, peer: str, reading: dict):
    try:
        await client.post(f"{peer.rstrip('/')}/sync/ingest", json=[reading])
    except httpx.HTTPError as e:
        log.warning("push to %s failed: %s", peer, e)


async def merge_with_peer(peer: str) -> dict:
    """Bidirectional one-shot merge: push everything we have, pull everything they have."""
    peer = peer.rstrip("/")
    local = await get_all_readings()

    pushed = 0
    pulled = 0
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        if local:
            r = await client.post(f"{peer}/sync/ingest", json=local)
            r.raise_for_status()
            pushed = r.json().get("inserted", 0)

        r = await client.get(f"{peer}/sync/dump")
        r.raise_for_status()
        remote = r.json()

    for reading in remote:
        if await ingest_reading(reading):
            pulled += 1

    return {"pushed": pushed, "pulled": pulled}
