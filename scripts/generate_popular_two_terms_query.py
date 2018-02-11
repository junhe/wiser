import sys

def create_database(database, output):
    header = database.readline()
    count = 0
    num_queries = 0
    data = database.readlines()
    dic = set()
    query = {}
    maxterm = 500000
    queries_need = 100000
    count = 0
    for i in range(maxterm-1):
        if count >= queries_need:
            break
        term1 = data[i].split(' ')[0]
        if term1 in dic:
            continue
        pos1 = data[i].split(' ')[2].strip(';\n').split(';')

        if count%10 == 0:
            print "==== Finished ", count, " terms"

        max_intersect = 0
        max_term2 = ''
        for j in range(i+1, maxterm):
            term2 = data[j].split(' ')[0]
            if term2 in dic:
                continue
            pos2 = data[j].split(' ')[2].strip(';\n').split(';')
            intersected = len(set(pos1).intersection(pos2))
            if intersected > max_intersect:
                max_intersect = intersected
                max_term2 = term2
                if max_intersect > 50: #len(pos2)*0.2:
                    break
        if max_intersect <= 50:
            continue
        count += 1
        query[term1 + ' ' + max_term2] = max_intersect
        #print dic
        dic.add(term1)
        dic.add(max_term2)
    
    li = sorted(query.iteritems(), key=lambda d:d[1], reverse = True)
    
    # create index
    length = len(li)

    # print database
    for word in li[0:max(len(li)-1,queries_need)]:
        output.write(word[0] + ':' +  str(word[1]) + '\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python generate_popular_two_terms.py [term_database_with_docIDs]')
        exit(1)
    
    # do analysis
    database = open(sys.argv[1])
    output = open('popular_two_terms_100000', 'w')

    create_database(database, output)
    
