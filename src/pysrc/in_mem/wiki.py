"""
Indexing and searching wikipedia
"""

from utils.expbase import Experiment
from .search_engine import *

class QueryPool(object):
    def __init__(self, query_path, n):
        self.fd = open(query_path, 'r')
        self.n = n
        # self.queries = self.fd.readlines(n)
        self.queries = []

        while True:
            line = self.fd.readline()
            if line == "":
                break

            self.queries.append(line)
            n -= 1
            if n == 0:
                break
        self.i = 0

    def next_query(self):
        ret = self.queries[self.i]
        self.i = (self.i + 1) % self.n
        # return ret
        return ret.split()[0]


class LineDocPool(object):
    def __init__(self, doc_path):
        self.fd = open(doc_path)

        # Get column names
        # The first line of the doc must be the header
        # Sample header:
        #   FIELDS_HEADER_INDICATOR###      doctitle        docdate body
        header = self.fd.readline()
        self.col_names = header.split("###")[1].split()

    def doc_iterator(self):
        for line in self.fd:
            yield self.line_to_dict(line)

    def line_to_dict(self, line):
        items = line.split("\t")
        return {k:v for k,v in zip(self.col_names, items)}


class ExperimentWiki(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "default-exp-name"

        self.doc_count = 10
        self.query_count = 100

    def before(self):
        self.query_pool = QueryPool("/mnt/ssd/downloads/wiki_QueryLog", self.query_count)
        self.doc_pool = LineDocPool("/mnt/ssd/work-large-wiki/linedoc")
        self.engine = Engine()

        for i, doc_dict in enumerate(self.doc_pool.doc_iterator()):
            if i == self.doc_count:
                break

            self.engine.index_writer.add_doc(doc_dict)

        # self.engine.index.display()

    def beforeEach(self, conf):
        pass

    def treatment(self, conf):
        for i in range(self.query_count):
            query = self.query_pool.next_query()
            doc_ids = self.engine.searcher.search([query], "AND")
            print doc_ids


def main():
    exp = ExperimentWiki()
    exp.main()


if __name__ == '__main__':
    main()


