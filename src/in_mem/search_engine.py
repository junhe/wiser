import os
import sys
import re

class Index(object):
    def __init__(self, input_dir):
        self.input_dir = input_dir
        self.build_index

    def build_index(self):
        print('building index from' + self.input_dir)


class Searcher(object):
    def __init__(self, index):
        print('created a searcher')

    def searchAND(self, phrase):
        print('querying '+ phrase)
        return ['./test_1', './test_2']


if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python search_engine.py [index_dir]')
        exit(1)

    input_dir = sys.argv[1]


    in_mem_index = Index(input_dir)

    in_mem_searcher = Searcher(in_mem_index)

    while(True):
        print('ready for searching:')
        read_in = 'test'  #TODO read in from command line
        for answer in in_mem_searcher.searchAND(read_in):
            print answer
        
