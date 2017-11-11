#from client import redis_client as client
import random
import string
import time
import asyncio
import aioredis
import signal
import sys
import redis
import threading

c_group = None
class client:    # async client
    def __init__(self, client_id, port, loop):
        self.loop = loop
        self.port = port
        self.client_id = client_id
    
    async def connect(self): 
        self.rredis  = await aioredis.create_redis(
            ('localhost', self.port), loop=self.loop)
    
    def close_connection(self):
        self.redis.close()
    
    async def query(self, num_requests, key_space):
        #redis = await aioredis.create_redis(
        #    ('localhost', self.port), loop=self.loop)
        redis = self.rredis
        for i in range(num_requests):
            key = int(key_space*self.client_id + i%key_space)
            val = await redis.get('pymemtier_'+str(key))
        redis.close()

class client_group:
    def __init__(self, num_clients, port):
        # config
        self.port = port
        self.loop_run = asyncio.get_event_loop()
        # create clients
        self.clients = []
        for i in range(num_clients):
            self.clients.append(client(i, port, self.loop_run))
        # wait for connect
            #asyncio.ensure_future(self.clients[i].connect())
        #pending = asyncio.Task.all_tasks()
        #self.loop_run.run_until_complete(asyncio.gather(*pending))
        self.loop_run.run_until_complete(asyncio.wait([client[0].query(num_requests, self.key_space_each_client)]))
        

    def populate(self, key_space, value_size):
        self.key_space = key_space

        print('========== populate the database')
        r = redis.Redis(
             host='localhost',
             port= self.port)
        value = ''.join(random.choice(string.ascii_lowercase) for x in range(value_size))
        print(value)
        for i in range(key_space):
            r.set('pymemtier_'+str(i), value)
        print('========== finish populate the database')
        self.key_space_each_client = self.key_space / len(self.clients)

    def query(self, num_requests):

        #for client in self.clients:
        #    asyncio.ensure_future(client.query(num_requests, self.key_space_each_client))

        #pending = asyncio.Task.all_tasks()
        #self.loop_run.run_until_complete(asyncio.gather(*pending))
        self.loop_run.run_until_complete(asyncio.wait([client[0].query(num_requests, self.key_space_each_client)]))

    def close(self):
        for client in self.clients:
            client.close_connection()
        self.loop_run.stop()
        print('get here!')

def signal_handler(signal, frame):  
    c_group.close()
    #loop.stop()
    sys.exit(0)

if __name__=='__main__':
    print('========== get in benchmark')
    signal.signal(signal.SIGINT, signal_handler)
    
    # configuration
    key_space = 100000
    value_size = 4096
    num_requests = 10000
    num_clients = 50
    num_threads = 4
    print('clients: ' + str(num_clients) + ' requests/client: ' + str(num_requests))
    
    #prepare clients
    c_groups = []
    for i in range(num_threads):
        c_groups.append(client_group(num_clients, 7777))
    
    # populate the database
    c_groups[0].populate(key_space, value_size)
    
    # get benchmark
    print('========== start query')
    threads = []
    start = time.time()

    for i in range(num_threads):
        t = threading.Thread(target=c_groups[i].query(num_requests))
        threads.append(t)
        t.start()

    #c_groups[0].query(num_requests)
    for i in range(num_threads):
        threads[i].join()

    end=time.time()
    print('take: ' + str(end-start) + 's')
    print('throuput: ' +  str(float(num_threads*num_requests*num_clients)/float(end-start)) + ' gets/second')
