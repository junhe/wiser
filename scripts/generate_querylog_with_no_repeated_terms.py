import sys
import itertools
from random import *

def generate_querylog(database, output):
    num_terms = 1000000
    
    length = int(database.readline().split(' ')[1])
    length = 2000000
    lines = database.readlines()
    
    # hint
    print 'database size:', length
    print 'num terms: ', num_terms

    terms_occured = set([-1])
    # generate query_log
    for i in range(num_terms):
        if (i%10000 == 0) :
            print i, "queris generated"
        
        # decide how many terms k
        n_terms = randint(2,2)

        if (i in terms_occured) :
            print("Error")
            exit(1)

        # first term
        query = lines[i*2].split(' ')[0] + ' ' + lines[i*2+1].split(' ')[0]
        '''
        terms_occured.add(i)
        #query = ''
        # random choose from k areas, no replication
        divide_area = int(length/n_terms)
        for j in range(1, n_terms):
        #for j in range(n_terms):
            term = -1
            while term in terms_occured:
                term = randint(j*divide_area, (j+1)*divide_area)
            #print term
            terms_occured.add(term)
            query += lines[term].split(' ')[0] + ' '
        ''' 
        output.write(query + '\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python generate_querylog_with_no_repeated_terms.py [database]')
        exit(1)
    
    # do analysis
    database = open(sys.argv[1])
    output = open('querylog_no_repeated', 'w')

    generate_querylog(database, output)
