import sys
import itertools

def generate_querylog(term_config, database, output):
    # read term_config
    num_terms = int(term_config.readline())
    term_frequency = filter(None, term_config.readline().strip('\n').split(' '))
    
    length = int(database.readline().split(' ')[1])
    lines = database.readlines()
    print 'database size:', length
    print 'num terms: ', num_terms

    listoftermlists = []
    # generate query_log
    for i in range(num_terms):
        # get frequency area
        start, end = term_frequency[i].split('-')
        start_pos = int(length*int(start)/100)
        end_pos = int(length*int(end)/100)
        print 'area ' ,i ,':', start_pos , ' ', end_pos
        # get terms
        cur_list = []
        for i in range(start_pos, end_pos):
            cur_list.append(lines[i].split(' ')[0])
        listoftermlists.append(list(cur_list))


    # intersect lists to generate queries
    count = 0
    for element in itertools.product(*listoftermlists):
       count += 1
       if count > 100000:
           break
       output.write(str(element).encode('utf8') + '\n') 

if __name__=='__main__':
    # print help
    if len(sys.argv)!=4:
        print('Usage: python generate_querylog_based_on_term_databse.py [term_config_file] [frequency] [database] [output_name]')
        print('       Eg. term_config_file:')
        print('                            3   #number of terms needed')
        print('                            0-10 20-30 40-100  # each term in what frequency area in percentage')
        exit(1)
    
    # do analysis
    term_config = open(sys.argv[1])
    database = open(sys.argv[2])
    output = open(sys.argv[3], 'w')

    generate_querylog(term_config, database, output)
    
