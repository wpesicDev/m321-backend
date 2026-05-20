import asyncio
import logging
from datetime import datetime, time, timedelta

from database import run_retention_aggregation

log = logging.getLogger("aggregation")

# Täglicher Lauf um 03:00 Uhr
AGGREGATION_RUN_AT = time(3, 0)


async def daily_aggregation_loop():
    while True:
        now = datetime.now()
        next_run = datetime.combine(now.date(), AGGREGATION_RUN_AT)
        if next_run <= now:
            next_run += timedelta(days=1)
        sleep_seconds = (next_run - now).total_seconds()
        log.info("next aggregation at %s (in %.0fs)", next_run.isoformat(), sleep_seconds)
        await asyncio.sleep(sleep_seconds)

        try:
            summary = await run_retention_aggregation()
            log.info("aggregation done: %s", summary)
        except Exception:
            log.exception("aggregation failed")
