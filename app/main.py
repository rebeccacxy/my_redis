import asyncio

class RedisServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        message = data.decode()
        if 'GET' in message:
            self.transport.write(b"$3\r\n")
            self.transport.write(b"BAZ\r\n")
        else:
            self.transport.write(b"-ERR unknown command\r\n")

def main():
    loop = asyncio.get_event_loop()
    coro = loop.create_server(RedisServerProtocol, '127.0.0.1', 6379)
    server = loop.run_until_complete(coro)

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
