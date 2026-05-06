import asyncio
import json
import logging

from database import save_reading

PORT = 8080
HOSTS = ["172.20.10.4"]
# "192.168.1.1"

KEYS = ["temp", "humi", "airp", "lum"]
INTERVAL = 3600.0

ERROR_MESSAGES = {
    0: "malformed request",
    1: "unknown keyword",
    2: "server out of resources",
}

log = logging.getLogger("sensor")
cache: dict[str, dict] = {}


async def query(host: str, request: str, timeout: float = 5.0) -> str:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, PORT), timeout
    )
    try:
        writer.write(f"{request}\n".encode())
        await writer.drain()

        response = await asyncio.wait_for(reader.readline(), timeout)
        return response.decode().strip()
    finally:
        writer.close()
        await writer.wait_closed()


def parse_response(response: str) -> dict:
    if response.startswith("e"):
        code = int(response[1:])
        return {
            "error": {
                "code": code,
                "message": ERROR_MESSAGES.get(code, "unknown"),
            }
        }

    values = json.loads(response)
    return dict(zip(KEYS, values))


async def query_key(host: str, key: str) -> dict:
    if key not in KEYS:
        return {"error": {"code": 1, "message": "unknown keyword"}}
    response = await query(host, key)
    if response.startswith("e"):
        code = int(response[1:])
        return {"error": {"code": code, "message": ERROR_MESSAGES.get(code, "unknown")}}
    values = json.loads(response)
    return {key: values[0]}


async def poll(host: str):
    request = ";".join(KEYS)

    while True:
        try:
            response = await query(host, request)
            result = parse_response(response)
            cache[host] = result

            if "error" in result:
                log.warning("%s -> %s", host, response)
            else:
                log.info("%s -> %s", host, result)
                try:
                    await save_reading(host, result)
                    log.info("saved to db: %s", host)
                except Exception as e:
                    log.error("db save failed: %s", e)

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            cache[host] = {"error": str(e) or type(e).__name__}
            log.error("%s unreachable: %s", host, type(e).__name__)

        await asyncio.sleep(INTERVAL)
