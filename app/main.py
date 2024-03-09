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

class RedisServerProtocol(asyncio.Protocol):
    def __init__(self, db):
        self._db = db
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        parsed = parse_wire_protocol(data)
        command = parsed[0].decode().lower()
        if command == 'get':
            response = self._db.get(parsed[1])
        elif command == 'set':
            response = self._db.set(parsed[1], parsed[2])

        wire_response = serialize_to_wire(response)
        self.transport.write(wire_response)

def main(hostname='localhost', port=6379):
    loop = asyncio.get_event_loop()
    db = redis_db.DB()
    protocol_factory = lambda: RedisServerProtocol(db)

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
