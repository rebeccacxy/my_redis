class DB:
    def __init__(self, db=None):
        if db is None:
            db = {}
        self._db = db

    def get(self, item):
        return self._db.get(item)
    
    def set(self, item, value):
        self._db[item] = value
        return True
    
    def rpush(self, item, values):
        curr_list = self._db.setdefault(item, [])
        if isinstance(values, bytes):
            values = values.decode()
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
            element = value.pop(0)
            return element
        return "WAIT"