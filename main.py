import asyncio
from fastapi import FastAPI, HTTPException

app = FastAPI()

async def fetch_from_tcp(host: str, port: int, message: bytes = b"", timeout: float = 5.0) -> bytes:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    try:
        if message:
            writer.write(message)
            await writer.drain()
        data = await asyncio.wait_for(reader.read(4096), timeout=timeout)
        return data
    finally:
        writer.close()
        await writer.wait_closed()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/tcp-data")
async def get_tcp_data():
    try:
        raw = await fetch_from_tcp("127.0.0.1", 9000, message=b"GET data\n")
        return {"data": raw.decode()}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="TCP connection timed out")
    except ConnectionRefusedError:
        raise HTTPException(status_code=502, detail="TCP connection refused")