from collections import deque
from collections import UserDict
import pickle
import multiprocessing

from server.conf.settings import PDB_FILE


class RedisDict(UserDict):
    def __missing__(self, key):
        return None


class RedisHash(UserDict):
    def __missing__(self, key):
        self[key] = RedisDict()
        return self[key]


class RedisSet(UserDict):
    def __missing__(self, key):
        self[key] = set()
        return self[key]


class RedisZset(UserDict):
    def __missing__(self, key):
        self[key] = set()
        return self[key]


class RedisList(UserDict):
    def __missing__(self, key):
        self[key] = deque()
        return self[key]


class RedisData:
    __slots__ = ('STR', 'HASH', 'SET', 'ZSET', 'LIST')

    # TODO 线程安全
    _obj = None

    def __init__(self):
        self.STR = RedisDict()
        self.HASH = RedisHash()
        self.SET = RedisSet()
        self.ZSET = RedisZset()
        self.LIST = RedisList()
        load(PDB_FILE)

    def __new__(cls, *args, **kwargs):
        if cls._obj is None:
            cls._obj = super().__new__(cls)
        return cls._obj

    def keys(self, pattern: str):
        keys = set(self.STR.keys())
        keys.update(self.HASH.keys())
        keys.update(self.LIST.keys())
        keys.update(self.ZSET.keys())

        def match(string):
            i = j = 0
            while i < len(pattern) and j < len(string):
                if pattern[i] == '*':
                    i += 1
                elif pattern[i] != string[j]:
                    j += 1
                else:
                    i += 1
                    j += 1

            if pattern[i:] != '*' * len(pattern[i:]):
                return False
            if j != len(string) and pattern[-1] != '*':
                return False
            return True

        return [key for key in keys if match(key)]

    def get(self, *keys):
        return [self.STR[key] for key in keys]

    def set(self, **mapping):
        self.STR.update(mapping)

    def hget(self, key, *fields):
        return [self.HASH[key][field] for field in fields]

    def hset(self, key, **mapping):
        self.HASH[key].update(mapping)

    def hlen(self, key):
        return len(self.HASH[key])

    def hkeys(self, key):
        return self.HASH[key].keys()

    def hgetall(self, key):
        return self.HASH[key].items()

    def lpush(self, key, values):
        for v in values:
            self.LIST[key].appendleft(v)
        return self.llen(key)

    def rpush(self, key, values):
        for v in values:
            self.LIST[key].append(v)
        return self.llen(key)

    def lpop(self, key):
        try:
            return self.LIST[key].popleft()
        except IndexError:
            return None

    def rpop(self, key):
        try:
            return self.LIST[key].pop()
        except IndexError:
            return None

    def llen(self, key):
        return len(self.LIST[key])

    def lindex(self, key, index):
        try:
            return self.LIST[key][index]
        except IndexError:
            return None

    def lset(self, key, index, value):
        try:
            self.LIST[key][index] = value
            return True
        except IndexError:
            return False

    def save(self):
        dump(self, PDB_FILE)

    def bgsave(self):
        work = multiprocessing.Process(target=dump, args=(self, PDB_FILE))
        # work.daemon = True
        work.start()
        # work.join()


def dump(obj, file):
    try:
        with open(file, 'wb') as f:
            pickle.dump(obj, f)
            print('success')
    except Exception as e:
        print(repr(e))


def load(file):
    try:
        with open(file, 'rb') as f:
            obj = pickle.load(f)
            return obj
    except Exception as e:
        print('恢复失败，初始化')
        print(repr(e))


if __name__ == '__main__':
    RD = RedisData()

    # RD.hset('b', a='b', b='b')
    # RD.set(a='b')
    # dump(RD, PDB_FILE)
    print(RD.get('a'))
    RD.save()
