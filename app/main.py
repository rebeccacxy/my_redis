import asyncio
import random
import app.redis_db as redis_db
import app.parser as parser
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

class ReplicaClient:
    def __init__(self, master_host: str, master_port: int) -> None:
        self._master_host = master_host
        self._master_port = master_port

    async def handshake(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._master_host, self._master_port
        )
        response = p.serialize_to_wire([types.PING.encode()])
        print("Response:", p.parse_wire_protocol(response))
        self._writer.write(response)
        await self._writer.drain()


class RedisServerProtocol(asyncio.Protocol):
    def __init__(self, client, db, key_blocker, pubsub, role, master_host, master_port) -> None:
        self._db = db
        self._key_blocker = key_blocker
        self._pubsub = pubsub
        self.transport = None
        self.role = role
        self.replid = random.randbytes(20).hex()
        self.repl_offset = 0
        self.master_host = master_host
        self.master_port = master_port
        self.client = client

    def connection_made(self, transport):
        self.transport = transport

    def subscribe(self, args):
        return self._pubsub.subscribe(args[0], self.transport)

    def ping(self, args):
        return 'PONG'

    def echo(self, args):
        return args[0].lower().decode()

    def publish(self, args):
        return self._pubsub.publish(args[0], args[1])

    def get(self, args):
        return self._db.get(args[0])

    def set(self, args):
        if len(args) == 2:
            return self._db.set(args[0], args[1])
        elif args[2].lower() == b'px':
            return self._db.set(args[0], args[1], int(args[3].decode()))

    def rpush(self, args):
        asyncio.create_task(self._key_blocker.data_for_key(args[0], args[1]))
        return self._db.rpush(args[0], args[1:])

    def lrange(self, args):
        return self._db.lrange(args[0], int(args[1]), int(args[2]))

    def blpop(self, args):
        response = self._db.blpop(args[0])
        if response == types.WAIT:
            queue = self._key_blocker.wait_for_key(args[0], self.transport)
            asyncio.create_task(queue)
            return
        
    def info(self, args):
        info = {
            'role': self.role,
            'master_replid': self.replid,
            'master_repl_offset': self.repl_offset
            }

        info = " ".join(f"{k}:{v}" for k, v in info.items())
        return info

    def parse_command(self, data):
        return p.parse_wire_protocol(data)

    def serialize_response(self, response):
        return p.serialize_to_wire(response)

    def data_received(self, data):
        parsed = p.parse_wire_protocol(data) # [command, arg1, arg2]
        command, args = parsed[0].lower().decode(), parsed[1:]
        response = None

        command_dict = {
            types.SUBSCRIBE: self.subscribe,
            types.PING: self.ping,
            types.ECHO: self.echo,
            types.PUBLISH: self.publish,
            types.GET: self.get,
            types.SET: self.set,
            types.RPUSH: self.rpush,
            types.LRANGE: self.lrange,
            types.BLPOP: self.blpop,
            types.INFO: self.info
        }

        response = command_dict.get(command)(args)
        serialized = p.serialize_to_wire(response)
        self.transport.write(serialized)

class ProtocolFactory:
    def __init__(self, protocol_cls, *args, **kwargs):
        self._protocol_cls = protocol_cls
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        return self._protocol_cls(*self._args, **self._kwargs)

async def main(hostname='localhost', port=6379):
    loop = asyncio.get_event_loop()

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
    server = await loop.create_server(protocol_factory,
                            hostname, port, start_serving=False)
    
    print("Listening on port {}".format(port))

    try:
        async with server:
            if role == roles.SLAVE:
                await ReplicaClient(master_host, master_port).handshake()
            await server.start_serving()
            await server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
