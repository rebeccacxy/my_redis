# my_redis

This is a pure-Python implementation of the Redis key-value store.

## Blocking Left Pop (BLPOP)
```Bash
Client 1: BLPOP foo 0 --> Redis
Client 2: BLPOP foo 0 --> Redis
Client 3: RPUSH foo bar
```
Since Client 1 and 2 are indefinitely waiting (`0` is a timeout), Client 3 will send `bar` to whichever client has been waiting the longest. 