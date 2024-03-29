import asyncio
import random
import app.parser as parser
from app.types import types

p = parser.Parser()

class RedisServerProtocol(asyncio.Protocol):
    def __init__(self, db, key_blocker, pubsub, role, master_host, master_port) -> None:
        self._db = db
        self._key_blocker = key_blocker
        self._pubsub = pubsub
        self.transport = None
        self.role = role
        self.replid = random.randbytes(20).hex()
        self.repl_offset = 0
        self.master_host = master_host
        self.master_port = master_port

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

    def data_received(self, data):
        parsed = p.parse_wire_protocol(data) # [command, arg1, arg2]
        print(parsed)
        if isinstance(parsed, str):
            command = parsed.lower()
            args = None
        else:
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
        

class RedisReplicationServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport
        self.send_ping()

    def send_ping(self):
        self.transport.write(p.serialize_to_wire([types.PING.encode()]))
        self.transport.close()

class ProtocolFactory:
    def __init__(self, protocol_cls, *args, **kwargs):
        self._protocol_cls = protocol_cls
        self._args = args
        self._kwargs = kwargs

    def __call__(self, *args, **kwargs):
        return self._protocol_cls(*self._args, **self._kwargs)