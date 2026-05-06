import asyncio
import json
import logging
import random
import socket
import struct
import time

MULTICAST_GROUP = "239.255.42.99"
DISCOVERY_PORT = 9999
HTTP_PORT = 8000
HEARTBEAT = 2.0
PEER_TIMEOUT = 10.0


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


NODE_ID = random.randint(1, 1_000_000)
NODE_URL = f"http://{get_local_ip()}:{HTTP_PORT}"

log = logging.getLogger("cluster")
state = {"is_leader": False}
peers: dict[int, float] = {}


def alive_peers() -> list[int]:
    now = time.time()
    return [pid for pid, seen in peers.items() if now - seen < PEER_TIMEOUT]


async def broadcast_loop():
    from tcp import cache

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    while True:
        was_leader = state["is_leader"]
        state["is_leader"] = not any(pid > NODE_ID for pid in alive_peers())
        if state["is_leader"] != was_leader:
            log.info("role -> %s", "leader" if state["is_leader"] else "follower")

        msg: dict = {"id": NODE_ID, "is_leader": state["is_leader"]}
        if state["is_leader"]:
            msg["cache"] = cache
        try:
            sock.sendto(json.dumps(msg).encode(), (MULTICAST_GROUP, DISCOVERY_PORT))
            if state["is_leader"]:
                log.info("synced cache to %d peer(s): %s", len(alive_peers()), cache)
        except Exception as e:
            log.warning("multicast send failed: %s", e)

        await asyncio.sleep(HEARTBEAT)


async def listen_loop():
    from tcp import cache

    loop = asyncio.get_event_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", DISCOVERY_PORT))
    mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.setblocking(False)
    log.info("node %d listening on multicast %s:%d", NODE_ID, MULTICAST_GROUP, DISCOVERY_PORT)
    while True:
        try:
            data, _ = await loop.sock_recvfrom(sock, 65535)
            payload = json.loads(data.decode())
            pid = payload["id"]
            if pid == NODE_ID:
                continue
            if pid not in peers:
                log.info("discovered peer %d", pid)
            peers[pid] = time.time()
            if payload.get("is_leader") and pid > NODE_ID and "cache" in payload:
                cache.clear()
                cache.update(payload["cache"])
                log.info("received cache from leader %d: %s", pid, payload["cache"])
        except Exception:
            pass
