import asyncio
import logging

from database import save_reading
from tcp import query, parse_response

HOSTS = ["172.20.10.4"]
KEYS = ["temp", "humi", "airp", "lum"]
INTERVAL = 3600.0

log = logging.getLogger("sensor")
cache: dict[str, dict] = {}


async def get_current_readings() -> dict[str, dict]:
    request = ";".join(KEYS)
    results = {}
    for host in HOSTS:
        try:
            response = await query(host, request)
            results[host] = parse_response(response, KEYS)
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            results[host] = {"error": str(e) or type(e).__name__}
    return results


async def poll(host: str):
    request = ";".join(KEYS)

    while True:
        try:
            response = await query(host, request)
            result = parse_response(response, KEYS)
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
