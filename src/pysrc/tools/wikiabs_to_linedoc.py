import sys

from utils.utils import WikiAbstract2



def main():
    if len(sys.argv) != 3:
        print "Usage: python -m tools.wikiabs_to_linedoc wiki-xml-path output-path"
        exit(1)

    wikipath = sys.argv[1]
    outpath = sys.argv[2]

    # wiki = WikiAbstract("utils/testdata/enwiki-abstract-sample.xml")
    wiki = WikiAbstract2(wikipath)

    f = open(outpath, "wb")
    f.write("FIELDS_HEADER_INDICATOR###      doctitle        body\n")

    for i, entry in enumerate(wiki.entries()):
        # print entry
        f.write(entry['title'].encode("utf-8"))
        f.write('\t')
        f.write(entry['abstract'].encode("utf-8"))
        f.write('\n')
        if i % 1000 == 0:
            print i

    f.close()

if __name__ == '__main__':
    main()


