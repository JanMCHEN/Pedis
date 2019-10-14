"""
数据组指形式，内存中的数据结构，多个hash表组成
"""

from collections import deque
from collections import UserDict
import pickle
import multiprocessing
import time

from server.conf.settings import PDB_FILE


class RedisDict(UserDict):
    """基本k-v结构，用于存放普通键值对，对应redis string类型"""
    def __missing__(self, key):
        return None


class RedisHash(UserDict):
    """对应hash结构，因为缺失时返回一个字典"""
    def __missing__(self, key):
        self[key] = RedisDict()
        return self[key]


class RedisList(UserDict):
    """对应list结构，缺失时返回队列用以支持redis操作"""
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
    """保存数据库中所有键，值为数据类型，方便查找，同时避免不同数据类型键重复。
    0-string，1-hash, 2-list, 3-set, 4-zset"""
    def __init__(self, *args):
        """
        :param args: 对应数据结构的引用，方便删除键
        """
        super().__init__()
        self.zip = args

        # 记录当前数据被修改次数，已对应采取不同同步策略
        self.modify = 0

    def __missing__(self, key):
        return None

    def pop(self, k):
        """默认删除对应数据结构中的数据"""
        if self[k] is not None:
            del self.zip[self[k]][k]
            del self[k]
            self.modify += 1
            return True
        return False


class RedisExpires(UserDict):
    """键过期时间设置，值为对应的过期时间戳"""
    def __init__(self, keys):
        super().__init__()
        self.KEYS = keys

    def __missing__(self, key):
        """如果key不在KEYS中，此时key不存在，否则key存在但是还未设置过期时间，返回不同值"""
        if self.KEYS[key] is None:
            return -2
        return -1

    def pop(self, k):
        """方便直接通过k删除对应键值"""
        self.KEYS.pop(k)
        del self[k]


class RedisData:
    # 减小类大小，同时设计为单例模式
    __slots__ = ('STRING', 'HASH', 'LIST', 'SET', 'ZSET', 'KEYS', 'EXPIRES')

    # TODO 线程不安全
    _obj = None

    def _init(self):
        # 只在未通过磁盘加载成功时初始化
        self.STRING = RedisDict()
        self.HASH = RedisHash()
        self.LIST = RedisList()
        self.SET = RedisSet()
        self.ZSET = RedisZset()

        self.KEYS = RedisKey(self.STRING, self.HASH, self.LIST, self.SET, self.ZSET)
        self.EXPIRES = RedisExpires(self.KEYS)

    def __new__(cls, *args, **kwargs):
        if cls._obj is None:
            cls._obj = super().__new__(cls)
            if load(PDB_FILE) is None:
                cls._obj._init()

        return cls._obj

    def _check_key(self, key, tp):
        """检查key对应值类型是否是给定类型"""
        real_tp = self.KEYS[key]
        if real_tp is None or real_tp == tp:
            return 1
        return 0

    def keys(self, pattern: str):
        """获取所有满足pattern的key"""

        def match(string):
            """结合redis的*通配符的字符串匹配函数，未经过大量测试"""
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
        """获取key值类型"""
        if self.KEYS[key] is not None:
            return self.__slots__[self.KEYS[key]].lower()
        return 'none'

    def del_(self, keys):
        """删除key"""
        count = 0
        for key in keys:
            count += self.KEYS.pop(key)
        return count

    def expire(self, key, seconds):
        """设置失效时间"""
        if self.EXPIRES[key] == -2:
            return False
        self.EXPIRES[key] = time.time() + seconds

        self.KEYS.modify += 1
        return True

    def persist(self, key):
        """取消失效设置，与Timer模块设计不同，只是单纯的从EXPIRES里删除，只是测试用"""
        if self.EXPIRES[key] < 0:
            return False

        self.KEYS.modify += 1
        del self.EXPIRES[key]

    def ttl(self, key):
        """获取距离失效时间，单位秒"""
        if self.EXPIRES[key] < 0:
            return self.EXPIRES[key]
        if self.EXPIRES[key] == 0:
            return -2
        return int(self.EXPIRES[key] - time.time() + 0.5)

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
            self.KEYS.modify += 1

    def hget(self, key, *fields):
        if not self._check_key(key, 1):
            return False
        return [self.HASH[key][field] for field in fields]

    def hset(self, key, **mapping):
        if not self._check_key(key, 1):
            return False
        self.KEYS[key] = 1
        self.HASH[key].update(mapping)
        self.KEYS.modify += 1
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
            self.KEYS.modify += 1
        return self.llen(key)

    def rpush(self, key, values):
        if not self._check_key(key, 2):
            return False

        self.KEYS[key] = 2

        for v in values:
            self.LIST[key].append(v)
            self.KEYS.modify += 1
        return self.llen(key)

    def lpop(self, key):
        if self.KEYS[key] is None:
            return None

        if self.KEYS[key] == 2:
            ret = self.LIST[key].popleft()
            self.KEYS.modify += 1

            if not self.LIST[key]:
                self.KEYS.pop(key)
            return ret
        return None

    def rpop(self, key):
        if self.KEYS[key] is None:
            return None

        if self.KEYS[key] == 2:
            ret = self.LIST[key].popleft()
            self.KEYS.modify += 1

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
            self.KEYS.modify += 1
            return 0
        except IndexError:
            return 1

    def save(self):
        dump(self, PDB_FILE)
        print('async over')

    def bgsave(self):
        # 由于linux和windows多进程实现机制不一样，在linux下性能更佳
        work = multiprocessing.Process(target=self.save)
        work.start()


def dump(obj, file):
    try:
        with open(file, 'wb') as f:
            pickle.dump(obj, f)
    except Exception as e:
        print(repr(e))


def load(file):
    try:
        with open(file, 'rb') as f:
            return pickle.load(f)
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
