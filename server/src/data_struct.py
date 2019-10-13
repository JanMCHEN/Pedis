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


class RedisList(UserDict):
    def __missing__(self, key):
        self[key] = deque()
        return self[key]


class RedisSet(UserDict):
    def __missing__(self, key):
        self[key] = set()
        return self[key]


class RedisZset(UserDict):
    def __missing__(self, key):
        self[key] = set()
        return self[key]


class RedisKey(UserDict):
    def __init__(self, *args):
        super().__init__()
        self.zip = args

    def __missing__(self, key):
        return None

    def pop(self, k):
        if self[k] is not None:
            del self.zip[self[k]][k]
            del self[k]
            return True
        return False


class RedisData:
    __slots__ = ('STRING', 'HASH', 'LIST', 'SET', 'ZSET', 'KEYS', 'EXPIRES')

    # TODO 线程不安全
    _obj = None

    def __init__(self):
        self.STRING = RedisDict()
        self.HASH = RedisHash()
        self.LIST = RedisList()
        self.SET = RedisSet()
        self.ZSET = RedisZset()

        self.KEYS = RedisKey(self.STRING, self.HASH, self.LIST, self.SET, self.ZSET)
        self.EXPIRES = RedisDict()

        load(PDB_FILE)

    def __new__(cls, *args, **kwargs):
        if cls._obj is None:
            cls._obj = super().__new__(cls)
        return cls._obj

    def _check_key(self, key, tp):
        real_tp = self.KEYS[key]
        if real_tp is None or real_tp == tp:
            return 1
        return 0

    def keys(self, pattern: str):

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

        return [key for key in self.KEYS if match(key)]

    def type_(self, key):
        if self.KEYS[key] is not None:
            return self.__slots__[self.KEYS[key]].lower()
        return 'none'

    def del_(self, keys):
        count = 0
        for key in keys:
            count += self.KEYS.pop(key)

        return count

    def get(self, m=False, *keys):
        if not m and not self._check_key(keys[0], 0):
            return False
        return [self.STRING[key] for key in keys]

    def set(self, **mapping):
        for k, v in mapping.items():
            if self.KEYS[k] is not None and self.KEYS[k] > 0:
                self.KEYS.pop(k)
            self.KEYS[k] = 0
            self.STRING[k] = v

    def hget(self, key, *fields):
        if not self._check_key(key, 1):
            return False
        return [self.HASH[key][field] for field in fields]

    def hset(self, key, **mapping):
        if not self._check_key(key, 1):
            return False
        self.KEYS[key] = 1
        self.HASH[key].update(mapping)
        return True

    def hlen(self, key):
        if not self._check_key(key, 1):
            return False
        return len(self.HASH[key])

    def hkeys(self, key):
        if not self._check_key(key, 1):
            return False
        return self.HASH[key].keys()

    def hgetall(self, key):
        if not self._check_key(key, 1):
            return False
        return self.HASH[key].items()

    def lpush(self, key, values):
        if not self._check_key(key, 2):
            return False

        self.KEYS[key] = 2

        for v in values:
            self.LIST[key].appendleft(v)
        return self.llen(key)

    def rpush(self, key, values):
        if not self._check_key(key, 2):
            return False

        self.KEYS[key] = 2

        for v in values:
            self.LIST[key].append(v)
        return self.llen(key)

    def lpop(self, key):
        if self.KEYS[key] is None:
            return None

        if self.KEYS[key] == 2:
            ret = self.LIST[key].popleft()
            if not self.LIST[key]:
                self.KEYS.pop(key)
            return ret
        return None

    def rpop(self, key):
        if self.KEYS[key] is None:
            return None

        if self.KEYS[key] == 2:
            ret = self.LIST[key].popleft()
            if not self.LIST[key]:
                self.KEYS.pop(key)
            return ret
        return None

    def llen(self, key):
        if not self._check_key(key, 2):
            return False
        return len(self.LIST[key])

    def lindex(self, key, index):
        if not self._check_key(key, 2):
            return False
        try:
            return self.LIST[key][index]
        except IndexError:
            return None

    def lset(self, key, index, value):
        if not self._check_key(key, 2):
            return -1
        try:
            self.LIST[key][index] = value
            return 0
        except IndexError:
            return 1

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
