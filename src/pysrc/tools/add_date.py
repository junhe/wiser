"""
Previously, wiki abstract linedoc does not have 'docdate' field, which cannot be fed
into Lucene benchmark. This script adds docdate to the linedoc.

The docdate are 'fake'. They are from wiki articles.
"""

from utils.utils import LineDocPool, QueryPool


def main():
    article_pool = LineDocPool("/mnt/ssd/work-large-wiki/linedoc")
    dates = []
    for i, doc_dict in enumerate(article_pool.doc_iterator()):
        dates.append(doc_dict['docdate'])

        if i % 10000 == 0:
            print i

        # if i == 100:
            # break

    # now we have all the dates
    f = open("/mnt/ssd/downloads/enwiki-abstract.linedoc.withdate", "w")
    f.write("FIELDS_HEADER_INDICATOR###doctitle\tdocdate\tbody\n")

    n = len(dates)
    abstract_pool = LineDocPool("/mnt/ssd/downloads/enwiki-abstract.linedoc")
    for i, doc_dict in enumerate(abstract_pool.doc_iterator()):
        docdate = dates[i % n]
        f.write(doc_dict["doctitle"])
        f.write("\t")
        f.write(docdate.strip())
        f.write("\t")
        f.write(doc_dict["body"])
        f.write("\n")

        if i % 10000 == 0:
            print i

        # if i == 100:
            # break

    f.close()

if __name__ == "__main__":
    main()


