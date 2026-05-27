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
INTERVAL = float(os.getenv("POLL_INTERVAL", "15"))
REQUEST = ";".join(KEYS)

log = logging.getLogger("sensor")
cache: dict[str, dict] = {}
_active_pollers: set[str] = set()


async def get_current_readings() -> dict:
    readings: list[tuple[str, dict]] = []
    errors: dict[str, dict | str] = {}
    for host in HOSTS:
        try:
            response = await query(host, REQUEST)
            result = parse_response(response, KEYS)
            if "error" in result:
                errors[host] = result["error"]
            else:
                readings.append((host, result))
        except Exception as e:
            errors[host] = str(e)

    if not readings:
        log.warning("no sensor hosts responded (errors: %s)", errors)
        return {"sources": [], "errors": errors}

    sources = [host for host, _ in readings]
    if len(sources) == len(HOSTS) and len(HOSTS) > 1:
        log.info("averaged readings from all %d hosts: %s", len(sources), sources)
    elif len(HOSTS) > 1:
        unreachable = [h for h in HOSTS if h not in sources]
        log.info("using readings from %s only (unreachable: %s)", sources, unreachable)
    else:
        log.info("readings from %s", sources)

    averaged: dict = {}
    for key in KEYS:
        values = [r[key] for _, r in readings if r.get(key) is not None]
        if values:
            averaged[key] = sum(values) / len(values)

    response: dict = {
        "sources": sources,
        **averaged,
    }
    if errors:
        response["errors"] = errors
    return response


async def poll(host: str):
    if host in _active_pollers:
        log.warning("poller already running for %s, skipping duplicate", host)
        return
    _active_pollers.add(host)

    try:
        while True:
            try:
                response = await query(host, REQUEST)
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
                                broadcast_reading(
                                    {"host": host, "timestamp": timestamp, **result}
                                )
                            )
                    except Exception as e:
                        log.error("db save failed: %s", e)

            except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
                cache[host] = {"error": str(e) or type(e).__name__}
                log.error("%s unreachable: %s", host, type(e).__name__)

            await asyncio.sleep(INTERVAL)
    finally:
        _active_pollers.discard(host)
