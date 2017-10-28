import sys

def create_database(line_doc, output):
    header = line_doc.readline()
    dic = {}
    for line in line_doc:
        items = line.split("\t")
        doc_content = filter(None, items[2].strip('\n').split(" "))
        for term in doc_content:
            dic.setdefault(term,0)
            dic[term] +=1

    li = sorted(dic.iteritems(), key=lambda d:d[1], reverse = True)
    
    # create index
    length = len(li)

    # print index
    output.write('len: ' + str(length) + '\n')
    # print database
    for word in li:
        output.write(word[0] + ' ' +str(word[1])+ '\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python generate_term_database.py [tokenized line_doc]')
        exit(1)
    
    # do analysis
    line_doc = open(sys.argv[1])
    output = open(sys.argv[1] + '_term_database', 'w')

    create_database(line_doc, output)
    
