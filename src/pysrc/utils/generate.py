
from utils import *
import unicodedata
import io
def generate():
    with io.open("output.linedoc", "w") as output:
        output.write(u'FIELDS_HEADER_INDICATOR###\tdoctitle\tbody\n')
        #wiki = Wiki("enwiki-latest-pages-articles1.xml")
        wiki = Wiki("enwiki-latest-pages-articles.xml")
        i = 0
        for entry in wiki.entries():
            if entry['redirect'] != None:
                continue
            i += 1
            if i % 10000 == 0:
                print "get ", i, "documents"
            #output.write(entry['title'] + '\t' + unidecode.unidecode( entry['text'].replace('\n', ' ') ) + '\n')
            #output.write(unicode(entry['title'] + '\n'))
            #print entry['title']
            output.write(unicode(entry['title'] + '\t' 
                                 + unicodedata.normalize('NFKD', unicode(entry['text'].replace('\n', ''))).encode('ascii', 'ignore') 
                                 + '\n'))

if __name__ == '__main__':
    generate()
