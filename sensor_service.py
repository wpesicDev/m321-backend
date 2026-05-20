import asyncio
import logging
import os

from dotenv import load_dotenv

from database import save_reading, aggregate_hourly, cleanup_old_readings
from sync_service import broadcast_reading
from tcp import query, parse_response

load_dotenv()

HOSTS = [h.strip() for h in os.getenv("SENSOR_HOSTS", "").split(",") if h.strip()]
KEYS = ["temp", "humi", "airp", "lum"]
INTERVAL = float(os.getenv("POLL_INTERVAL", "60.0"))

log = logging.getLogger("sensor")
cache: dict[str, dict] = {}


async def _query_host(host: str) -> tuple[str, dict]:
    request = ";".join(KEYS)
    try:
        response = await query(host, request)
        return host, parse_response(response, KEYS)
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
        return host, {"error": str(e) or type(e).__name__}


async def get_current_readings() -> dict:
    responses = await asyncio.gather(*(_query_host(host) for host in HOSTS))

    readings: list[tuple[str, dict]] = []
    errors: dict[str, dict | str] = {}
    for host, result in responses:
        if "error" in result:
            errors[host] = result["error"]
        else:
            readings.append((host, result))

    if not readings:
        return {"sources": [], "errors": errors}

    averaged: dict = {}
    for key in KEYS:
        values = [r[key] for _, r in readings if r.get(key) is not None]
        if values:
            averaged[key] = sum(values) / len(values)

    response: dict = {
        "sources": [host for host, _ in readings],
        **averaged,
    }
    if errors:
        response["errors"] = errors
    return response


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


async def hourly_maintenance():
    """Run hourly maintenance: aggregate readings and cleanup old data."""
    while True:
        try:
            await asyncio.sleep(3600)  # Run every hour
            log.info("starting hourly aggregation and cleanup")
            await aggregate_hourly()
            await cleanup_old_readings()
            log.info("hourly maintenance completed")
        except Exception as e:
            log.error("hourly maintenance failed: %s", e)
