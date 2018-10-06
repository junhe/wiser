import sys
import os
from pyreuse.helpers import *


dict_file = {}
stats_file = {}

def analyze_lba(filename):
    this_blocks = set()
    last_block = -1
    with open('/tmp/lba_dump', 'r') as lba_dump:
        hit_head = False
        for line in lba_dump:
            if 'byte_offset' in line:
                hit_head = True
                continue
            if hit_head == False:
                continue
            items = line.strip('\n').split()
            start_block = (int(items[1])-46139392)/8
            end_block = (int(items[2])-46139392+7)/8
            for block in range(start_block, end_block):
                this_blocks.add(block)
            #print items[0], items[1], items[2]
    dict_file[filename] = this_blocks

def handle_directory(dir_name):
    for filename in os.listdir(dir_name):
        if 'fake' in filename or 'segments_1o' in filename or 'lock' in filename:
            continue
        shcmd('hdparm --fibmap ' + dir_name + filename + ' > /tmp/lba_dump')
        analyze_lba(filename)




blktrace_file_name = sys.argv[1]
interested_directory_name = sys.argv[2]

handle_directory(interested_directory_name)
#print dict_file
#exit(1)
with open(blktrace_file_name, 'r') as blktrace_file:
    for line in blktrace_file:
        items = line.strip('\n').split(' ')
        operation = items[1]
        byte_offset = int(items[2])
        size = int(items[3])
        block_offset = byte_offset/4096

        if operation != 'read':
            continue
        found = False
        for file_name in dict_file:
            if block_offset in dict_file[file_name]:
                found = True
                break
        if found == True:
            #print block_offset, size, file_name
            if file_name not in stats_file:
                stats_file[file_name] = size
            else:
                stats_file[file_name] += size
        else:
            print block_offset, size, 'else'

overall_size = 0
for key in stats_file:
    overall_size += stats_file[key]
    print key, ' : ', float(stats_file[key])/1024/1024/1024, ' GB'

print 'overall: ', float(overall_size)/1024/1024/1024, ' GB'
