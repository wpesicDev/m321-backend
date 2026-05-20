import asyncio
import logging
import os

import httpx

from database import get_all_readings, get_peers, ingest_reading

log = logging.getLogger("sync")
TIMEOUT = 10.0
PEER_SYNC_INTERVAL = float(os.getenv("PEER_SYNC_INTERVAL", "60"))


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


async def periodic_peer_sync():
    """Background loop: periodically reconcile with every registered peer.

    Why: broadcast_reading is fire-and-forget, so any push that fails while a
    peer is offline is lost. This loop fills the gap by re-running a full
    bidirectional merge on an interval — once the peer is reachable again,
    missed readings flow both ways.
    """
    while True:
        try:
            peers = await get_peers()
            for peer in peers:
                try:
                    result = await merge_with_peer(peer)
                    if result["pushed"] or result["pulled"]:
                        log.info("periodic merge with %s: %s", peer, result)
                except httpx.HTTPError as e:
                    log.warning("periodic merge with %s failed: %s", peer, e)
                except Exception as e:
                    log.error("periodic merge with %s errored: %s", peer, e)
        except Exception as e:
            log.error("periodic_peer_sync loop error: %s", e)

        await asyncio.sleep(PEER_SYNC_INTERVAL)
