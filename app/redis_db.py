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