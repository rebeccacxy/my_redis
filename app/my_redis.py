import asyncio
import io

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
        # There's a '\r\n' that comes after a bulk string
        # so we .readline() to move passed that crlf.
        msg_buffer.readline()
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
    def __init__(self, redis):
        self._redis = redis
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        parsed = parse_wire_protocol(data)
        # parsed is an array of [command, *args]
        command = parsed[0].decode().lower()
        try:
            method = getattr(self._redis, command)
        except AttributeError:
            self.transport.write(
                b"-ERR unknown command " + parsed[0] + b"\r\n")
            return
        result = method(*parsed[1:])
        serialized = serialize_to_wire(result)
        self.transport.write(serialized)

class WireRedisConverter(object):
    def __init__(self, redis):
        self._redis = redis

    def lrange(self, name, start, end):
        return self._redis.lrange(name, int(start), int(end))

    def hmset(self, name, *args):
        converted = {}
        iter_args = iter(list(args))
        for key, val in zip(iter_args, iter_args):
            converted[key] = val
        return self._redis.hmset(name, converted)

    def __getattr__(self, name):
        return getattr(self._redis, name)