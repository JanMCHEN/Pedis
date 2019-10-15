"""
服务器核心代码
"""


import asyncio

from server.src.resp_code import Resp
from server.src.data_struct import *
from server.src.timer import Timer


# 数据初始化
DB = RedisData()
TIME = Timer()

# 支持命令列表
COMMAND = {
    'KEYS', 'DEL', 'TYPE', 'EXPIRE', 'PERSIST', 'TTL',
    'GET', 'MGET', 'SET', 'MSET', 'STRLEN',
    'HSET', 'HGET', 'HMSET', 'HMGET', 'HLEN', 'HKEYS', 'HGETALL',
    'LPUSH', 'RPUSH', 'LPOP', 'RPOP', 'LLEN', 'LINDEX', 'LSET',
    'SADD', 'SPOP', 'SCARD', 'SMEMBERS', 'SREM',
    'SAVE', 'BGSAVE',

}


class PedisServer:
    """简易版redis服务器实现"""
    def __init__(self, reader, writer, database=None):
        self.reader = reader
        self.writer = writer
        self.db = database

    @staticmethod
    def command_process(data, opt='*'):
        """解析'*，$'等后面跟的数字"""
        if data[0] == opt and data[1:].isdigit():
            return int(data[1:])
        return 0

    async def recv(self):
        """接收数据，直到\r\n"""
        try:
            data = await self.reader.readuntil(separator=b'\r\n')
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return -1
        return data.decode().strip('\r\n')

    async def _handle(self):
        """每个客户端连接时的回调函数"""
        while True:
            # 接收*...，表示接下来会收到几个指令
            data = await self.recv()
            if data == -1:
                break

            count = self.command_process(data.strip())

            if count < 1:
                continue

            # 接收第一个命令前表示命令长度的数据
            await self.recv()

            # 第一个命令
            com = (await self.recv()).upper()

            # 根据第一个命令选择处理方式
            if com not in COMMAND:
                info = Resp.error(f'unknown command {com}')
            else:
                info = await getattr(self, com)(count-1)

            # 返回给客户端
            self.writer.write(info)
            try:
                await self.writer.drain()
            except ConnectionResetError:
                continue

    async def get_key(self):
        """用于接收1个键值"""
        r = await self.recv()
        length = self.command_process(r, '$')
        return (await self.reader.readexactly(length + 2)).decode().strip('\r\n')

    async def get_key_value(self, count):
        """用于接收1个键值对"""
        mapping = {}
        for i in range(count):
            k = await self.get_key()
            v = await self.get_key()
            mapping[k] = v
        return mapping

    async def TYPE(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for TYPE command')

        key = await self.get_key()

        return Resp.ok(self.db.type_(key))

    async def DEL(self, count):
        if count < 1:
            return Resp.error('wrong number of arguments for TYPE command')

        keys = {await self.get_key() for _ in range(count)}

        return Resp.integer(self.db.del_(keys))

    async def KEYS(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for KEYS command')
        pattern = await self.get_key()
        return Resp.encode(self.db.keys(pattern))

    async def EXPIRE(self, count):
        if count != 2:
            return Resp.error('wrong number of arguments for EXPIRE command')
        key = await self.get_key()

        seconds = await self.get_key()

        try:
            seconds = int(seconds)
        except ValueError:
            return Resp.error('value is not an integer')

        if self.db.expire(key, seconds):
            TIME.expire(key)
            return Resp.integer(1)
        return Resp.integer(0)

    async def PERSIST(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for PERSIST command')
        key = await self.get_key()
        return Resp.integer(TIME.persist(key))

    async def TTL(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for TTL command')
        key = await self.get_key()

        return Resp.integer(self.db.ttl(key))

    async def MGET(self, count, opt='*'):
        if count < 1:
            return Resp.error('wrong number of arguments for MGET command')

        keys = [await self.get_key() for _ in range(count)]

        m = True if opt == '*' else False

        return Resp.encode(self.db.get(m, *keys), opt)

    async def GET(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for GET command')
        return await self.MGET(1, '$')

    async def MSET(self, count):
        if count % 2:
            return Resp.error('wrong number of arguments for MSET')

        mapping = await self.get_key_value(count//2)

        self.db.set(**mapping)
        return Resp.ok()

    async def SET(self, count):
        if count < 2:
            return Resp.error('wrong number of arguments for SET command')
        key = await self.get_key()
        val = await self.get_key()

        cmd = [await self.get_key() for _ in range(count-2)]

        if not cmd:
            self.db.set(**{key: val})
            return Resp.ok()

        if len(cmd) == 2 and cmd[0].lower() == 'ex':
            try:
                sec = int(cmd[1])
            except ValueError:
                return Resp.error('value is not an integer')
            self.db.set(**{key: val})
            self.db.expire(key, sec)
            TIME.expire(key)
            return Resp.ok()
        return Resp.error('syntax error')

    async def HMGET(self, count, opt='*'):
        if count < 2:
            return Resp.error('wrong number of arguments for HMGET command')
        key = await self.get_key()

        fields = [await self.get_key() for _ in range(count-1)]

        return Resp.encode(self.db.hget(key, *fields), opt)

    async def HGET(self, count):
        if count != 2:
            return Resp.error('wrong number of arguments for HGET command')
        return await self.HMGET(2, '$')

    async def HMSET(self, count):
        count -= 1
        if count % 2:
            return Resp.error('ERR wrong number of arguments for HMSET')

        key = await self.get_key()

        mapping = await self.get_key_value(count//2)

        if len(mapping) == count // 2:
            return Resp.encode(self.db.hset(key, **mapping))

        return Resp.error('fail')

    async def HSET(self, count):
        if count != 3:
            return Resp.error('wrong number of arguments for HSET command')
        return await self.HMSET(3)

    async def HLEN(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for HLEN command')
        key = await self.get_key()

        return Resp.encode(self.db.hlen(key))

    async def HKEYS(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for HKEYS command')
        key = await self.get_key()
        return Resp.encode(self.db.hkeys(key))

    async def HGETALL(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for HGETALL command')
        key = await self.get_key()

        info = []
        for k, v in self.db.hgetall(key):
            info.append(k)
            info.append(v)
        return Resp.encode(info)

    async def LPUSH(self, count):
        if count < 2:
            return Resp.error('wrong number of arguments for LPUSH command')
        key = await self.get_key()

        values = [await self.get_key() for _ in range(count-1)]

        return Resp.encode(self.db.lpush(key, values))

    async def RPUSH(self, count):
        if count < 2:
            return Resp.error('wrong number of arguments for RPUSH command')
        key = await self.get_key()

        values = [await self.get_key() for _ in range(count - 1)]

        return Resp.encode(self.db.rpush(key, values))

    async def LPOP(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for LPOP command')
        key = await self.get_key()
        return Resp.encode(self.db.lpop(key), '$')

    async def RPOP(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for RPOP command')
        key = await self.get_key()
        return Resp.encode(self.db.rpop(key), '$')

    async def LLEN(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for LLEN command')
        key = await self.get_key()
        return Resp.encode(self.db.llen(key))

    async def LINDEX(self, count):
        if count != 2:
            return Resp.error('wrong number of arguments for LLEN command')

        key = await self.get_key()
        index = await self.get_key()

        try:
            index = int(index)
        except ValueError:
            return Resp.error('value is not an integer')
        return Resp.encode(self.db.lindex(key, index), '$')

    async def LSET(self, count):
        if count != 3:
            return Resp.error('wrong number of arguments for LSET command')

        key = await self.get_key()
        index = await self.get_key()

        try:
            index = int(index)
        except ValueError:
            return Resp.error('value is not an integer')

        value = await self.get_key()

        ret = self.db.lset(key, index, value)

        if ret == 0:
            return Resp.ok()
        if ret == 1:
            return Resp.error('index out of range')
        if ret == -1:
            return Resp.encode(False)

    async def SADD(self, count):
        if count < 2:
            return Resp.error('wrong number of arguments for SADD command')

        key = await self.get_key()

        members = [await self.get_key() for _ in range(count-1)]

        return Resp.encode(self.db.sadd(key, members))

    async def SPOP(self, count):
        if count == 0 or count > 2:
            return Resp.error('wrong number of arguments for SPOP command')

        key = await self.get_key()

        if count == 1:
            return Resp.encode(self.db.spop(key), '$')

        try:
            count = int(await self.get_key())
        except ValueError:
            return Resp.error('value is not an integer')
        ret = [self.db.spop(key) for _ in range(count)]

        return Resp.encode(ret)

    async def SCARD(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for SCARD command')
        key = await self.get_key()
        return Resp.encode(self.db.scard(key))

    async def SMEMBERS(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for SMEMBERS command')

        key = await self.get_key()

        return Resp.encode(self.db.smembers(key))

    async def SREM(self, count):
        if count < 2:
            return Resp.error('wrong number of arguments for REM command')

        key = await self.get_key()

        values = {await self.get_key() for _ in range(count-1)}

        return Resp.encode(self.db.srem(key, values))

    async def SAVE(self, *args):
        self.db.save()
        return Resp.ok()

    async def BGSAVE(self, *args):
        self.db.bgsave()
        return Resp.ok('Background saving started')


async def handle(reader, writer):
    server = PedisServer(reader, writer, DB)
    client = writer.get_extra_info('peername')
    # print(f'get new client {client!r}')
    await server._handle()
    # print(client, 'leave')
    writer.close()


async def main():
    server = await asyncio.start_server(handle, '127.0.0.1', '12345')

    host = server.sockets[0].getsockname()
    print(f'Serving on {host}')

    Timer(asyncio.get_running_loop())

    try:
        async with server:
            await server.serve_forever()
    finally:
        DB.save()

if __name__ == '__main__':
    asyncio.run(main())


