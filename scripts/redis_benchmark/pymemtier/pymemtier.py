from client import redis_client as client
import time

if __name__=='__main__':
    print('==== get in benchmark')
    # configuration
    key_space = 100000
    value_size = 8
    num_requests = 100000

    c = client(7777)
    
    # populate the database
    c.populate(key_space, value_size)
    # get benchmark
    start = time.time()
    c.query(num_requests, key_space)
    end=time.time()
    print('throuput: ' +  str(num_requests/float(end-start)) + ' gets/second')
