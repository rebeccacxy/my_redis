import asyncio
import app.redis_db as redis_db
import app.parser as parser
from app.types import types

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

    def data_received(self, data):
        parsed = p.parse_wire_protocol(data) # [command, arg1, arg2]
        command = parsed[0].lower().decode()
        response = None
        if len(parsed) == 1:
            print("Error: Missing value")
            response = "Error: Missing value"
        
        if command == types.SUBSCRIBE:
            response = self._pubsub.subscribe(parsed[1], self.transport)
        elif command == types.PUBLISH:
            response = self._pubsub.publish(parsed[1], parsed[2])
        elif command == types.GET:
            response = self._db.get(parsed[1])
        elif command == types.SET:
            response = self._db.set(parsed[1], parsed[2])
        elif command == types.RPUSH:
            response = self._db.rpush(parsed[1], parsed[2:])
            asyncio.create_task(self._key_blocker.data_for_key(parsed[1], parsed[2]))
        elif command == types.LRANGE:
            response = self._db.lrange(parsed[1], int(parsed[2]), int(parsed[3]))
        elif command == types.BLPOP:
            response = self._db.blpop(parsed[1])
            if response == types.WAIT:
                queue = self._key_blocker.wait_for_key(parsed[1], self.transport)
                asyncio.create_task(queue)
                return

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
