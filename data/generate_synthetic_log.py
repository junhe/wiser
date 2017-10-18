import os
import sys
from collections import deque


def generate(input_file, output_file, window_limit, size_limit):
    text = input_file.readlines()
    count = 0
    if window_limit == 0:
        for each_query in text:
            output_file.write(each_query)
            count += 1
            if count >= size_limit:
                break
        return

    ele_window = deque([''])
    for each_query in text:
        # read in each query
        # judge whether contained in current window
        if each_query not in ele_window:
            if len(ele_window)==window_limit:
                ele_window.popleft()
            # if not, update window
            ele_window.append(each_query)
            output_file.write(each_query)
            count += 1
            if count>=size_limit:
                return


if __name__=='__main__':
    # print help
    if len(sys.argv)!=5:
        print('Usage: python generate_synthetic_log.py [log_file] [output_name] [locality_parameter] [size]')
        print('       [locality_parameter]: during how many lines+1 of queries, there will be no same query')
        print('                             0-MAX (integer, 0 means the raw log, MAX means no replicated queries)')
        print('       [size]              : max number of queries we want')
        exit(1)

    
    # do parse
    log_file = open(sys.argv[1])
    output_file = open(sys.argv[2],'w')
    if sys.argv[3]!='MAX':
        window = int(sys.argv[3])
    else:
        window = -1
    size = int(sys.argv[4])
 
    # generate according to locality parameter
    generate(log_file, output_file, window, size)
    
    log_file.close()
    output_file.close() 
