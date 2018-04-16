import os
import sys
input_file = sys.argv[1]

d = [0]*4097


def dump(dic):
    sum_page = 0
    for i in range(4097):
        if i % 256 == 0:
            print i, sum_page
            sum_page = 0
        sum_page += dic[i]
        #print i, ', ', d[i]

with open(input_file, 'r') as pages: 
    for line in pages:
        items = line.split()
        if len(items) < 2:
            #print d
            dump(d)
            d = [0]*4097
            print('\n' + line)
            #print line
            continue
        else:
            #print('.', end= '')
            #print items
            d[int(items[1])] += 1
                        

dump(d)
