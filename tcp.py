import asyncio
import json
import os

from dotenv import load_dotenv

load_dotenv()

PORT = int(os.getenv("SENSOR_PORT", "8080"))

ERROR_MESSAGES = {
    0: "malformed request",
    1: "unknown keyword",
    2: "server out of resources",
}


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


def parse_response(response: str, keys: list[str]) -> dict:
    if response.startswith("e"):
        code = int(response[1:])
        return {
            "error": {
                "code": code,
                "message": ERROR_MESSAGES.get(code, "unknown"),
            }
        }

    values = json.loads(response)
    return dict(zip(keys, values))
