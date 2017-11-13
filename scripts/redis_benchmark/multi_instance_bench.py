from multiprocessing import Process
import subprocess

def test(port):
    subprocess.call(['sh', '/users/kanwu/memtier_benchmark/get_benchmark.sh', '10000', str(port)])

# start query
num_instance = 2
processes = []
ports = [7778, 7779]
for i in range(num_instance):
    t = Process(target=test(ports[i]))
    processes.append(t)
    t.start()

for i in range(num_instance):
    processes[i].join()

