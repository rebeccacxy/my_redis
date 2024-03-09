# my_redis

This is a pure-Python implementation of the Redis key-value store.

## Functionality
- GET
- SET
- RPUSH
- LRANGE
- LPOP
- BLPOP

## Blocking Left Pop (BLPOP)
```Bash
Client 1: BLPOP foo
Client 2: BLPOP foo
Client 3: RPUSH foo bar
```
Since Client 1 and 2 are indefinitely waiting, Client 3 will send `bar` to whichever client has been waiting the longest. 

## TODO
- Implement expiry