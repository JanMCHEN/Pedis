import redis


coon = redis.Redis(port=12345)

print(coon.mset({'a': 'b'}), coon.set('b', 'b')), coon.hmset('a', {'a':'b'})
print(coon.get('a'), coon.get('b'), coon.mget('a', 'b'), )
coon.save()

coon.close()
