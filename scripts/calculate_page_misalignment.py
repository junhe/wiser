import os
import sys

file_to_parse = sys.argv[1]
d = {}
d_off = {}
all_length = 0
unique_length = 0

def print_for_each_file():
    for file_name in d:
        print file_name
        for page in sorted(d[file_name]):
            print page, len(d[file_name][page])#, ' : ', d[file_name][page]

def addto(file_name, offset, length):
    for i in range(length):
        page = (offset + i)/4096
        in_page_offset =  (offset + i) % 4096
        #print page, in_page_offset
        #add to dictionary of this file
        if file_name not in d:
            d[file_name] = {}
        if page not in d[file_name]:
            d[file_name][page] = set()

        d[file_name][page].add(in_page_offset)

def judge_unique(file_name, offset, length):
    unique = 0
    for i in range(length):
        cur_off = offset + i
        if file_name not in d_off:
            d_off[file_name] = set()
        if cur_off in d_off[file_name]:
            continue
        unique += 1
        d_off[file_name].add(cur_off)

    return unique

with open(file_to_parse, 'r') as reads:
    i = 0
    for line in reads:
        i += 1
        #if i%10000000 == 0:
        #    print 'Done: ', i
        items = line.split()
        if len(items) != 3:
            continue
        file_name = items[0]
        offset = int(items[1])
        length = int(items[2])
        #all_length += length
        #unique_length += judge_unique(file_name, offset, length)
        #print file_name, offset, length

        addto(file_name, offset, length)

#print 'overall_length: ', all_length
#print 'unique_length: ', unique_length
#print d['MMapIndexInput(path=\"/mnt/ssd/elasticsearch/data/nodes/0/indices/g9xojlsbSz2_x91y_m2a3g/0/index/_fxq_Lucene50_0.pos\")']

print_for_each_file()
