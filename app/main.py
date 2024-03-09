import asyncio
import io
import app.redis_db as redis_db

def parse_wire_protocol(message):
    return _parse_wire_protocol(io.BytesIO(message))

def _parse_wire_protocol(msg_buffer):
    current_line = msg_buffer.readline()
    msg_type, remaining = chr(current_line[0]), current_line[1:]
    if msg_type == '+':
        return remaining.rstrip(b'\r\n').decode()
    elif msg_type == ':':
        return int(remaining)
    elif msg_type == '$':
        msg_length = int(remaining)
        if msg_length == -1:
            return None
        result = msg_buffer.read(msg_length)
        msg_buffer.readline() # move past \r\n
        return result
    elif msg_type == '*':
        array_length = int(remaining)
        return [_parse_wire_protocol(msg_buffer) for _ in range(array_length)]
    
def serialize_to_wire(value):
    if isinstance(value, str):
        return ('+%s' % value).encode() + b'\r\n'
    elif isinstance(value, bool) and value:
        return b"+OK\r\n"
    elif isinstance(value, int):
        return (':%s' % value).encode() + b'\r\n'
    elif isinstance(value, bytes):
        return (b'$' + str(len(value)).encode() +
                b'\r\n' + value + b'\r\n')
    elif value is None:
        return b'$-1\r\n'
    elif isinstance(value, list):
        base = b'*' + str(len(value)).encode() + b'\r\n'
        for item in value:
            base += serialize_to_wire(item)
        return base
    
class PubSub:
    def __init__(self):
        self._channels = {}

    def subscribe(self, channel, transport):
        self._channels.setdefault(channel, []).append(transport)
        return ['subscribe', channel, 1]

    def publish(self, channel, message):
        transports = self._channels.get(channel, [])
        message = serialize_to_wire(
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
        transport.write(serialize_to_wire(value))

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
        parsed = parse_wire_protocol(data) # [command, arg1, arg2]
        command = parsed[0].lower().decode()
        response = None
        if len(parsed) == 1:
            print("Error: Missing value")
            response = "Error: Missing value"
        
        if command == "subscribe":
            response = self._pubsub.subscribe(parsed[1], self.transport)
        elif command == "publish":
            response = self._pubsub.publish(parsed[1], parsed[2])
        elif command == "get":
            response = self._db.get(parsed[1])
        elif command == "set":
            response = self._db.set(parsed[1], parsed[2])
        elif command == "rpush":
            response = self._db.rpush(parsed[1], parsed[2:])
            asyncio.create_task(self._key_blocker.data_for_key(parsed[1], parsed[2]))
        elif command == "lrange":
            response = self._db.lrange(parsed[1], int(parsed[2]), int(parsed[3]))
        elif command == "blpop":
            response = self._db.blpop(parsed[1])
            if response == "MUST WAIT":
                queue = self._key_blocker.wait_for_key(parsed[1], self.transport)
                asyncio.create_task(queue)
                return

        print(response)
        serialized = serialize_to_wire(response)
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
