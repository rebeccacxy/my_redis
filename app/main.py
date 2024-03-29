import asyncio
import app.redis_db as redis_db
import app.parser as parser
from app.server import RedisReplicationServerProtocol, RedisServerProtocol, ProtocolFactory
from app.types import types, roles
import argparse

p = parser.Parser()
    
class PubSub:
    def __init__(self):
        self._channels = {}

    def subscribe(self, channel, transport):
        self._channels.setdefault(channel, []).append(transport)
        return ['subscribe', channel, 1]

    def publish(self, channel, message):
        transports = self._channels.get(channel, [])
        message = p.serialize_to_wire(
            ['message', channel, message])
        for transport in transports:
            transport.write(message)
        return len(transports)

class KeyBlocker:
    def __init__(self):
        self._blocked_keys = {}

    async def wait_for_key(self, key, transport):
        if key not in self._blocked_keys:
            self._blocked_keys[key] = asyncio.Queue()

        queue = self._blocked_keys[key]
        value = await queue.get() # block until data available
        transport.write(p.serialize_to_wire(value))

    async def data_for_key(self, key, value):
        if key in self._blocked_keys:
            queue = self._blocked_keys[key]
            await queue.put(value)
            print("%s put in queue!", value)

    
async def start_server(protocol_factory, hostname, port):
    server = await asyncio.start_server(protocol_factory,
                            hostname, port)
    async with server:
        await server.serve_forever()

async def start_replica_server(hostname, port):
    await asyncio.sleep(1)  # ensure server is up
    _, _ = await asyncio.get_event_loop().create_connection(
        RedisReplicationServerProtocol, hostname, port)

async def main(hostname='localhost', port=6379):
    argParser = argparse.ArgumentParser(description="Start server")
    argParser.add_argument("--port", dest="port", default=6379)
    argParser.add_argument("--replicaof", nargs=2)
    args = argParser.parse_args()

    if args.port:
        port = args.port

    role = roles.SLAVE if args.replicaof else roles.MASTER
    master_host, master_port = None, None
    if args.replicaof:
        master_host = args.replicaof[0]
        master_port = args.replicaof[1]

    protocol_factory = ProtocolFactory(RedisServerProtocol, redis_db.DB(), KeyBlocker(), PubSub(), role, master_host, master_port)
    loop = asyncio.get_event_loop()
    server = await loop.create_server(protocol_factory,
                            hostname, port)
    try:
        async with server:
            if role == roles.SLAVE:
                await start_replica_server(master_host, master_port)
            await server.start_serving()
            await server.serve_forever()
    finally:
        server.close()
        await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
