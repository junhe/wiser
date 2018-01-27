import sys
import io
import unicodedata

def expand(line_doc, output):
    header = line_doc.readline()
    # write header
    output.write(unicode('<feed>\n'))
    # parse each line
    i = 0
    for line in line_doc:
        i+=1
        if i % 10000 == 0:
            print 'Finished ', i, 'documents'
        items = line.split("\t")

        doc_content = unicodedata.normalize('NFKD', unicode(items[1])).encode('ascii', 'ignore')
        doc_title = unicodedata.normalize('NFKD', unicode(items[0])).encode('ascii', 'ignore')
        
        # write out
        output.write(unicode('<doc>\n'))
        # title
        output.write(unicode('<title>'))
        output.write(unicode(doc_title))
        output.write(unicode('</title>\n'))

        # url
        output.write(unicode('<url>'))
        output.write(unicode('http://fakeurl' + str(i)))
        output.write(unicode('</url>\n'))

        #links
        #output.write('<links>\n<sublink linktype="nav"><anchor>Etymology and terminology</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Etymology_and_terminology</link></sublink>\n</links>\n')
        # abstract
        output.write(unicode('<abstract>'))
        #output.write('test')
        doc_content = doc_content.strip('\n').replace('&', '').replace('<','').replace('>','')#.replace('\"','').replace('\'','')
        #doc_content = (doc_content[:10000] + '.') if len(doc_content) > 10000 else doc_content
        #doc_content = doc_content[45899:45951] + '.'
        output.write(unicode(doc_content))
        output.write(unicode(' </abstract>\n'))
        output.write(unicode('</doc>\n'))
    '''
    for i in range(10):
        # write out
        output.write('<doc>\n')
        # title
        output.write('<title>')
        output.write('title'+str(i))
        output.write('</title>\n')

        # url
        output.write('<url>')
        output.write('http://fakeurl' + str(i))
        output.write('</url>\n')
        #links
        #output.write('<links>\n<sublink linktype="nav"><anchor>Etymology and terminology</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Etymology_and_terminology</link></sublink>\n</links>\n')
        # abstract

        for j in range(20000):
            output.write('Hello. ')
        output.write('<abstract>')
        output.write(' </abstract>\n')
        output.write('</doc>\n')
    '''
    # write ending
    output.write(unicode('</feed>\n'))

if __name__ == '__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python generate_xml_for_rs_bench_go.py [input_name]  (results stored in input_name_expanded)')
        exit(1)
    
    # do analysis
    #output = io.open(sys.argv[1] + '_expanded', 'w', encoding="utf-8")
    #output = open(sys.argv[1] + '.xml', 'w')
    output = io.open(sys.argv[1] + '.xml', 'w', encoding="utf-8")
    #with open(sys.argv[1], "r") as line_doc:
    with io.open(sys.argv[1], "r", encoding="utf-8") as line_doc:
        expand(line_doc, output)
    #line_doc.close()
    output.close()
