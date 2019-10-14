"""
pedis配置文件
"""
import os


# 数据保存到磁盘位置，可以填具体绝对路径
PDB_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'dump.pdb')


# ------------同步机制---------------------- #
# 为保证最佳性能请不要乱修改

# 每隔多少秒检测到有数据变化即同步
ASYNC_TIME = 100

# 1秒内检测到多少数据变化立即同步1次
SEC_COUNT = 100

# 1分钟内检测到多少数据变化立即同步1次
MIN_COUNT = 10

# ----------------------------------------- #
