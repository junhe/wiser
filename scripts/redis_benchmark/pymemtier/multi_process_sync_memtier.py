from client import redis_client as client
import time
#import threading
from multiprocessing import Process
from multiprocessing.dummy import Pool as ThreadPool 
num_requests = 0
key_space = 0

def query(client):
    client.query(num_requests, key_space)

if __name__=='__main__':
    print('==== get in benchmark')
    # configuration
    key_space = 100000
    value_size = 4096
    num_requests = 1000000
    num_threads = 2

    c = []
    for i in range(num_threads):
        c.append(client(7777))
    
    # populate the database
    c[0].populate(key_space, value_size)
    # get benchmark


    processes = []
    pool = ThreadPool(num_threads) 

    start = time.time()

    results = pool.map(query, c)
    #for i in range(num_threads):
    #    t = Process(target=c[i].query(num_requests, key_space))
    #    processes.append(t)
    #    t.start()

    #c[0].query(num_requests, key_space)
    #for i in range(num_threads):
    #    processes[i].join()
    
    pool.close()
    pool.join()

    end=time.time()
    print('throuput: ' +  str(num_threads*num_requests/float(end-start)) + ' gets/second')
