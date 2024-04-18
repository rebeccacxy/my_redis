# my_redis

This is a pure-Python implementation of Redis (just a small subset of its features). I learn about event loops and the asyncio library here.

Redis is an in-memory key-value store, which can hold values that are not restricted to strings (lists, hashes, sets, etc.). It is efficient due to its in-memory data structures and single-threaded architecture. 

## Implementation

The idea is to use `asyncio` to provide a server that accepts client connections that can parse the wire protocol for redis. There is one `RedisServerProtocol` per client connection, which stores state that is scoped to the lifetime of that connection. The protocol class is handed to `create_server`, which is called on an event loop instance.

**Event loop**
```
Event loop 
--> wait_for_key --> queue.get() --> yield --> future
--> value for key --> queue.put() --> value unlocks future --> yield on value --> resume execution
```

## Functionality

- GET
- SET
    - `SET foo bar px 2000` sets expiry of `key = foo` to 2 seconds
- PUBLISH
- SUBSCRIBE
- RPUSH
- LRANGE
- LPOP
- BLPOP
    ```Bash
    Client 1: BLPOP foo
    Client 2: BLPOP foo
    Client 3: RPUSH foo bar
    ```
    Since Client 1 and 2 are indefinitely waiting, Client 3 will send `bar` to whichever client has been waiting the longest. 
- Server replication

## TODO
- PSYNC
- RDB persistence

## How to run
```bash
./run_server.sh

```
Then install and run `redis-cli` 