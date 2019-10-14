"""
服务器核心代码
"""


import asyncio

from server.src.resp_code import Resp
from server.src.data_struct import *
from server.src.timer import Timer


DB = RedisData()
TIME = Timer()

COMMAND = {
    'KEYS', 'DEL', 'TYPE', 'EXPIRE', 'PERSIST', 'TTL',
    'GET', 'MGET', 'SET', 'MSET', 'STRLEN',
    'HSET', 'HGET', 'HMSET', 'HMGET', 'HLEN', 'HKEYS', 'HGETALL',
    'LPUSH', 'RPUSH', 'LPOP', 'RPOP', 'LLEN', 'LINDEX', 'LSET',
    'SAVE', 'BGSAVE',

}


class PedisServer:
    """简易版redis服务器"""
    def __init__(self, reader, writer, database=None):
        self.reader = reader
        self.writer = writer
        self.db = database

    @staticmethod
    def command_process(data, opt='*'):
        if data[0] == opt and data[1:].isdigit():
            return int(data[1:])
        return 0

    async def recv(self):
        try:
            data = await self.reader.readuntil(separator=b'\r\n')
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return -1
        return data.decode().strip('\r\n')

    async def _handle(self):
        while True:
            data = await self.recv()
            if data == -1:
                break

            count = self.command_process(data.strip())

            if count < 1:
                continue

            await self.recv()

            com = (await self.recv()).upper()

            if com not in COMMAND:
                info = Resp.error(f'unknown command {com}')
            else:
                info = await getattr(self, com)(count-1)

            self.writer.write(info)
            try:
                await self.writer.drain()
            except ConnectionResetError:
                continue

    async def get_key(self):
        r = await self.recv()
        length = self.command_process(r, '$')
        return (await self.reader.readexactly(length + 2)).decode().strip('\r\n')

    async def get_key_value(self, count):
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


