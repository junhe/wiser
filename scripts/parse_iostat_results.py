import os

with open('iostat.out', 'r') as io_results:
    io_results.readline()
    i = 0
    cpu_usage = iops = bandwidth = 0
    for line in io_results:
        if i%6 == 2:
            items = line.strip('\n').split()
            cpu_usage = float(items[0]) + float(items[2])
        if i%6 == 5:
            items = line.strip('\n').split()
            iops = float(items[3])
            bandwidth = float(items[5]) / 1024
            print cpu_usage, iops,  bandwidth
        i += 1
        
