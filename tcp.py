import asyncio
import json

TCP_HOST = "127.0.0.1"
TCP_PORT = 9000


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await reader.read(1024)
    payload = json.dumps({
        "sensor": "temp-01",
        "temperature": 22.5,
        "humidity": 58.3,
        "unit": "celsius",
    })
    writer.write(payload.encode())
    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, TCP_HOST, TCP_PORT)
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
