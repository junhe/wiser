import sys

def create_database(line_doc, output):
    header = line_doc.readline()
    dic = {}
    count = 0
    for line in line_doc:
        count += 1
        if (count % 10000) == 0:
            print "===Finished ", count, " documents"
        items = line.split("\t")
        doc_content = filter(None, items[2].strip('\n').split(" "))
        for term in doc_content:
            dic.setdefault(term,[])
            dic[term].append(count)

    li = sorted(dic.iteritems(), key=lambda d:len(d[1]), reverse = True)
    
    # create index
    length = len(li)

    # print index
    output.write('OVERALL: ' + str(length) + '\n')
    # print database
    for word in li:
        positions = ''
        for pos in word[1]:
            positions += str(pos) + ';'
        output.write(word[0] + ' ' +  str(len(word[1])) + ' ' + positions + '\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python generate_term_database.py [tokenized line_doc]')
        exit(1)
    
    # do analysis
    line_doc = open(sys.argv[1])
    output = open(sys.argv[1] + '_term_database_with_docIDs', 'w')

    create_database(line_doc, output)
    
