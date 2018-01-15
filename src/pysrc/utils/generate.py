from utils import *

def generate():
    wiki = WikiAbstract2("enwiki-latest-pages-articles1.xml")
    #wiki = WikiAbstract2("enwiki-latest-pages-articles1.xml")
    entries = list(wiki.entries())
    for entry in entries:
        print "this entry: ", entry['title']
if __name__ == '__main__':
    generate()
