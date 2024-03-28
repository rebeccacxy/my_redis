import asyncio
import app.redis_db as redis_db
import app.parser as parser
from app.types import types
import sys

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

class RedisServerProtocol(asyncio.Protocol):
    def __init__(self, db, key_blocker, pubsub):
        self._db = db
        self._key_blocker = key_blocker
        self._pubsub = pubsub
        self.transport = None

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
        return 'role:master'

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
    
def main(hostname='localhost', port=6379):
    loop = asyncio.get_event_loop()
    protocol_factory = ProtocolFactory(RedisServerProtocol, redis_db.DB(), KeyBlocker(), PubSub(),)

    if len(sys.argv) >= 2 and sys.argv[1] == "--port":
        port = int(sys.argv[2])

    coro = loop.create_server(protocol_factory,
                              hostname, port)
    server = loop.run_until_complete(coro)
    print("Listening on port {}".format(port))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()

if __name__ == "__main__":
    main()
