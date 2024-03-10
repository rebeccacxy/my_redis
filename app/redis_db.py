from app.types import types
import time

class DB:
    def __init__(self, db=None):
        if db is None:
            db = {}
        self._db = db

    def get(self, key):
        result = self._db.get(key)
        if result is None:
            return
        
        (value, expiry) = result
        if expiry is None:
            return value
        else:
            # key has timed out, remove from db
            if int(time.time() * 1000) > int(expiry.decode()):
                del self._db[key]

        return value
    
    def set(self, key, value, ttl=None):
        if ttl is None:
            self._db[key] = (value, ttl)
            return True
        
        expiry = str(int(ttl.decode()) * 1000 + int(time.time() * 1000))
        self._db[key] = (value, expiry.encode())
        return True
    
    def rpush(self, key, values):
        # TODO: add expiry for rpush 
        curr_list = self._db.setdefault(key, [])
        curr_list.extend(values)
        return len(curr_list)
    
    def lrange(self, key, start, stop):
        if stop == -1:
            end = None
        else:
            stop += 1
        return self._db.get(key, [])[start:stop]
    
    def lpop(self, key):
        value = self._db.get(key, [])
        if value:
            return value.pop(0)
        
    def blpop(self, key):
        value = self._db.get(key, [])
        if value:
            if isinstance(value, bytes):
                value = value.decode()
            element = value.pop(0)
            return element
        return types.WAIT