import sys
import signal
import asyncio
import aioredis

loop = asyncio.get_event_loop()

async def client(client_id):
    print('client_' + str(client_id))
    redis = await aioredis.create_redis(
        ('localhost', 7777), loop=loop)
    #await redis.set(str(client_id) + 'my-key', str(client_id) + 'value')
    for i in range(50):
        val = await redis.get('pymemtier_' +str(client_id))
        print(client_id)
    redis.close()
    #await redis.wait_closed()

def signal_handler(signal, frame):  
    loop.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

for i in range(50):
    asyncio.ensure_future(client(i))

#loop.run_forever()
pending = asyncio.Task.all_tasks()
loop.run_until_complete(asyncio.gather(*pending))

print('get here')
# will print 'value'
