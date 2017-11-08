import redis
import random
import string

class redis_client:
    def __init__(self, port):
        ## Connect to redis server
        self.r = redis.Redis(
               host='localhost',
               port= port)

    def populate(self, keyspace, value_size):
        value = ''.join(random.choice(string.lowercase) for x in range(value_size))
        print value
        for i in range(keyspace):
            self.r.set('pymemtier_'+str(i), value)
   
    def query(self, num_requests, keyspace):
        for i in range(keyspace):
            value = self.r.get('pymemtier_'+str(i))
            if value == None:
                print value

    def set(self, key, value):
        ## Start Setting or Getting
        self.r.set(key, value)
    
    def get(self, key):
        ## Start Setting or Getting
        value = self.r.get(key)
        print(value)
