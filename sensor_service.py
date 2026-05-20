import asyncio
import logging
import os

from dotenv import load_dotenv

from database import save_reading
from sync_service import broadcast_reading
from tcp import query, parse_response

load_dotenv()

HOSTS = [h.strip() for h in os.getenv("SENSOR_HOSTS", "").split(",") if h.strip()]
KEYS = ["temp", "humi", "airp", "lum"]
INTERVAL = float(os.getenv("POLL_INTERVAL", "3600"))

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
                    timestamp = await save_reading(host, result)
                    if timestamp is None:
                        log.info("duplicate, skipped broadcast: %s", host)
                    else:
                        log.info("saved to db: %s", host)
                        asyncio.create_task(
                            broadcast_reading({"host": host, "timestamp": timestamp, **result})
                        )
                except Exception as e:
                    log.error("db save failed: %s", e)

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            cache[host] = {"error": str(e) or type(e).__name__}
            log.error("%s unreachable: %s", host, type(e).__name__)

        await asyncio.sleep(INTERVAL)
