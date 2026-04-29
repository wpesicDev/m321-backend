import asyncio
import json
import logging

PORT = 8080
HOSTS = ["172.20.10.14"]
# "192.168.1.1"

KEYS = ["temp", "humi", "airp", "lum"]
INTERVAL = 5.0

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

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            cache[host] = {"error": str(e) or type(e).__name__}
            log.error("%s unreachable: %s", host, type(e).__name__)

        await asyncio.sleep(INTERVAL)
