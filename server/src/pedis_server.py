import asyncio

from server.src.resp_code import Resp
from server.src.data_struct import *


db = RedisData()
COMMAND = {
    'KEYS',
    'GET', 'MGET', 'SET', 'MSET',
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

            com = await self.recv()

            if com.upper() not in COMMAND:
                info = Resp.error(f'unknown command {com}')
            else:
                info = await getattr(self, com.lower())(count-1)

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

    async def keys(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for KEYS command')
        pattern = await self.get_key()
        return Resp.encode(self.db.keys(pattern))

    async def mget(self, count, opt='*'):
        if count < 1:
            return Resp.error('wrong number of arguments for MGET command')

        keys = [await self.get_key() for _ in range(count)]

        return Resp.encode(self.db.get(*keys), opt)

    async def get(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for GET command')
        return await self.mget(1, '$')

    async def mset(self, count):
        if count % 2:
            return Resp.error('wrong number of arguments for MSET')

        mapping = await self.get_key_value(count//2)

        if len(mapping) == count // 2:
            self.db.set(**mapping)
            return Resp.ok()
        return Resp.error('fail')

    async def set(self, count):
        if count != 2:
            await self.reader.read()
            return Resp.error('wrong number of arguments for SET command')
        return await self.mset(2)

    async def hmget(self, count, opt='*'):
        if count < 2:
            return Resp.error('wrong number of arguments for HMGET command')
        key = await self.get_key()

        fields = [await self.get_key() for _ in range(count-1)]

        return Resp.encode(self.db.hget(key, *fields), opt)

    async def hget(self, count):
        if count != 2:
            return Resp.error('wrong number of arguments for HGET command')
        return await self.hmget(2, '$')

    async def hmset(self, count):
        count -= 1
        if count % 2:
            return Resp.error('ERR wrong number of arguments for HMSET')

        key = await self.get_key()

        mapping = await self.get_key_value(count//2)

        if len(mapping) == count // 2:
            self.db.hset(key, **mapping)
            return Resp.ok()
        return Resp.error('fail')

    async def hset(self, count):
        if count != 3:
            return Resp.error('wrong number of arguments for HSET command')
        return await self.hmset(3)

    async def hlen(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for HLEN command')
        key = await self.get_key()
        return Resp.integer(self.db.hlen(key))

    async def hkeys(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for HKEYS command')
        key = await self.get_key()
        return Resp.encode(self.db.hkeys(key))

    async def hgetall(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for HGETALL command')
        key = await self.get_key()

        info = []
        for k, v in self.db.hgetall(key):
            info.append(k)
            info.append(v)
        return Resp.encode(info)

    async def lpush(self, count):
        if count < 2:
            return Resp.error('wrong number of arguments for LPUSH command')
        key = await self.get_key()

        values = [await self.get_key() for _ in range(count-1)]

        return Resp.integer(self.db.lpush(key, values))

    async def rpush(self, count):
        if count < 2:
            return Resp.error('wrong number of arguments for RPUSH command')
        key = await self.get_key()

        values = [await self.get_key() for _ in range(count - 1)]

        return Resp.integer(self.db.rpush(key, values))

    async def lpop(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for LPOP command')
        key = await self.get_key()
        return Resp.encode([self.db.lpop(key)], '$')

    async def rpop(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for RPOP command')
        key = await self.get_key()
        return Resp.encode([self.db.rpop(key)], '$')

    async def llen(self, count):
        if count != 1:
            return Resp.error('wrong number of arguments for LLEN command')
        key = await self.get_key()
        return Resp.integer(self.db.llen(key))

    async def lindex(self, count):
        if count != 2:
            return Resp.error('wrong number of arguments for LLEN command')

        key = await self.get_key()
        index = await self.get_key()

        try:
            index = int(index)
        except ValueError:
            return Resp.error('value is not an integer')
        return Resp.encode([self.db.lindex(key, index)], '$')

    async def lset(self, count):
        if count != 3:
            return Resp.error('wrong number of arguments for LSET command')

        key = await self.get_key()
        index = await self.get_key()

        try:
            index = int(index)
        except ValueError:
            return Resp.error('value is not an integer')

        value = await self.get_key()

        if self.db.lset(key, index, value):
            return Resp.ok()
        return Resp.error('index out of range')

    async def save(self, *args):
        self.db.save()
        return Resp.ok()

    async def bgsave(self, *args):
        self.db.bgsave()
        return Resp.ok('Background saving started')


async def handle(reader, writer):
    server = PedisServer(reader, writer, db)
    client = writer.get_extra_info('peername')
    # print(f'get new client {client!r}')
    await server._handle()
    # print(client, 'leave')
    writer.close()


async def main():
    # 启动服务器
    server = await asyncio.start_server(handle, '127.0.0.1', '12345')

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())


