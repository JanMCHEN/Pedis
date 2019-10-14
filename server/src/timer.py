"""
处理时间事件，包括数据同步策略，键失效时间处理
"""
import asyncio
import time

from server.src.data_struct import RedisData
from server.conf.settings import *

DB = RedisData()


class Timer:
    _keys = DB.EXPIRES
    _task = {}

    def __init__(self, loop=None):
        if loop is not None:
            asyncio.set_event_loop(loop)
            self.async_task()
            self.save_task()

    def async_task(self):
        """首次启动时执行，目的是把过期的键删除，还未过期的添加计时事件"""
        for k in list(self._keys.keys()):
            if self._keys[k] > time.time():
                self._task[k] = asyncio.create_task(self.down(k))
            else:
                self._keys.pop(k)

    async def down(self, key):
        """失效计时"""
        try:
            await asyncio.sleep(self._keys[key]-time.time())

            # 删除策略-------立即删除
            self._keys.pop(key)

            # 清除任务
            del self._task[key]

        except asyncio.CancelledError:
            # 取消过期事件
            del self._task[key]

    def persist(self, key):
        """取消对键的失效设置"""
        if key in self._task:
            self._task[key].cancel()
            del self._keys[key]
            return 1
        return 0

    def expire(self, key):
        """设置键失效时间"""

        # 如果当前键存在于⏲事件中，则取消并重新开始新计时事件
        if key in self._task:
            self._task[key].cancel()

        self._task[key] = asyncio.create_task(self.down(key))

    async def save_any(self, t=100):
        """
        每t秒有数据变化时保存
        :param t:
        :return:
        """
        while True:
            await asyncio.sleep(t)
            if DB.KEYS.modify > 0:
                DB.bgsave()
                DB.KEYS.modify = 0

    async def save_per_sec(self, count):
        """
        每秒最少count个变化时保存
        :param count:
        :return:
        """
        while True:
            await asyncio.sleep(1)
            if DB.KEYS.modify >= count:
                DB.bgsave()
                DB.KEYS.modify = 0

    async def save_per_min(self, count):
        """
        没分钟最少count个变化时保存
        :param count: int
        :return:
        """
        while True:
            await asyncio.sleep(60)
            if DB.KEYS.modify >= count:
                DB.bgsave()
                DB.KEYS.modify = 0

    def save_task(self):
        """数据持久化策略"""
        DB.KEYS.modify = 0
        asyncio.create_task(self.save_any(ASYNC_TIME))
        asyncio.create_task(self.save_per_sec(SEC_COUNT))
        asyncio.create_task(self.save_per_min(MIN_COUNT))
