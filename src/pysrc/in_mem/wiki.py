"""
Indexing and searching wikipedia
"""

import datetime
import pickle

from utils.expbase import Experiment
from .search_engine import *
from pyreuse import helpers

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


def save_engine(engine, path):
    fd = open(path, "w")
    pickle.dump(engine, fd)
    fd.close()


def load_engine(path):
    fd = open(path)
    return pickle.load(fd)


class ExperimentWiki(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "default-exp-name"

        self.doc_count = 100
        self.query_count = 100
        self.engine_path = "/mnt/ssd/search-engine-cache"
        helpers.shcmd("rm -f " + self.engine_path)
        self.read_engine_cache = True
        self.update_engine_cache = True

    def setup_engine(self):
        if os.path.exists(self.engine_path):
            print "Loading engine from cache"
            self.engine = load_engine(self.engine_path)
        else:
            print "Building new engine..."
            self.engine = self.build_engine()
            if self.update_engine_cache:
                print "Updating engine cache"
                save_engine(self.engine, self.engine_path)

        self.engine.index.display()

    def build_engine(self):
        # doc_pool = LineDocPool("./in_mem/testdata/linedoc-sample")
        doc_pool = LineDocPool("/mnt/ssd/work-large-wiki/linedoc")
        engine = Engine()

        for i, doc_dict in enumerate(doc_pool.doc_iterator()):
            if i == self.doc_count:
                break

            engine.index_writer.add_doc(doc_dict)
            if i % 10 == 0:
                print "Progress: {}/{}".format(i, self.doc_count), datetime.datetime.now()

        return engine

    def before(self):
        self.query_pool = QueryPool("/mnt/ssd/downloads/wiki_QueryLog", self.query_count)
        self.setup_engine()

    def beforeEach(self, conf):
        self.starttime = datetime.datetime.now()

    def treatment(self, conf):
        for i in range(self.query_count):
            query = self.query_pool.next_query()
            doc_ids = self.engine.searcher.search([query], "AND")

    def afterEach(self, conf):
        self.endtime = datetime.datetime.now()
        duration = (self.endtime - self.starttime).total_seconds()
        print "Duration:", duration
        print "Query per second:", self.query_count / duration


def main():
    exp = ExperimentWiki()
    exp.main()


if __name__ == '__main__':
    main()


