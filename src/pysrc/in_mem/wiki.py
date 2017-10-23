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


def build_engine(doc_count, line_doc_path):
    """
    This function supports two types of line docs:
        1. line doc produced by Lucene benchmark
        2. line doc with the "terms" column
    """
    doc_pool = LineDocPool(line_doc_path)
    engine = Engine()

    if 'terms' in doc_pool.col_names:
        with_terms = True
    else:
        with_terms = False

    start = datetime.datetime.now()
    for i, doc_dict in enumerate(doc_pool.doc_iterator()):
        if i == doc_count:
            break

        if with_terms is True:
            terms = doc_dict['terms'].split(",")
            del doc_dict['terms']
            engine.index_writer.add_doc_with_terms(doc_dict, terms)
        else:
            engine.index_writer.add_doc(doc_dict)

        if i % 100 == 0:
            duration  = (datetime.datetime.now() - start).total_seconds()
            print "Progress: {}/{}".format(i, doc_count), \
                    'Duration:', duration, \
                    'Speed:', round(i / duration), \
                    datetime.datetime.now()

    return engine


class ExperimentWiki(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "iter-doc-count"

        self.engine_cache_path = "/mnt/ssd/search-engine-cache"
        # helpers.shcmd("rm -f " + self.engine_cache_path)
        self.read_engine_cache = False
        self.update_engine_cache = False

    def setup_engine(self, conf):
        if self.read_engine_cache is True and os.path.exists(self.engine_cache_path):
            print "Loading engine from cache"
            self.engine = load_engine(self.engine_cache_path)
        else:
            print "Building new engine..."
            self.engine = build_engine(conf['doc_count'], "/mnt/ssd/work-large-wiki/linedoc-with-tokens")
            if self.update_engine_cache:
                print "Updating engine cache"
                save_engine(self.engine, self.engine_cache_path)
        print "Got the engine :)"

    def conf(self, i):
        # return {'doc_count': 10**(i+3),
        return {'doc_count': 10**(i+1),
                'query_count': 10000
                }

    def before(self):
        self.result_table = []

    def beforeEach(self, conf):
        self.query_pool = QueryPool("/mnt/ssd/downloads/wiki_QueryLog", conf['query_count'])
        self.setup_engine(conf)

        self.starttime = datetime.datetime.now()

    def treatment(self, conf):
        for i in range(conf['query_count']):
            query = self.query_pool.next_query()
            doc_ids = self.engine.searcher.search([query], "AND")

    def afterEach(self, conf):
        self.endtime = datetime.datetime.now()
        duration = (self.endtime - self.starttime).total_seconds()
        print "Duration:", duration
        query_per_sec = conf['query_count'] / duration
        print "Query per second:", query_per_sec
        d = {
            "duration": duration,
            "query_per_sec": query_per_sec,
            }
        d.update(conf)

        self.result_table.append(d)

    def after(self):
        print self.result_table
        perf_path = os.path.join(self._subexpdir, "perf.txt")
        helpers.table_to_file(self.result_table, perf_path, width=0)


def preprocess():
    pool = LineDocPool("/mnt/ssd/work-large-wiki/linedoc")
    tokenizer = NltkTokenizer()

    out_f = open("/mnt/ssd/work-large-wiki/linedoc-with-tokens", "w")
    out_f.write("FIELDS_HEADER_INDICATOR###      doctitle        docdate body terms\n")

    cnt = 0
    for doc_dict in pool.doc_iterator():
        doc_terms = []
        for k, v in doc_dict.items():
            terms = tokenizer.tokenize(str(v))
            doc_terms.extend(terms)
        doc_terms = list(set(doc_terms))
        doc_terms = ",".join(doc_terms)
        # line_items = [doc_dict["doctitle"], doc_dict["docdate"], doc_dict["body"], doc_terms]
        # line = doc_dict["doctitle"] + "\t" + doc_dict["docdate"] + "\t" + \
                # doc_dict["body"] + "\t" + doc_terms
        out_f.write(doc_dict["doctitle"])
        out_f.write("\t")
        out_f.write(doc_dict["docdate"])
        out_f.write("\t")
        out_f.write(doc_dict["body"].strip())
        out_f.write("\t")
        out_f.write(doc_terms)
        out_f.write("\n")
        cnt += 1

        if cnt % 100 == 0:
            print cnt

    out_f.close()


def main():
    if len(sys.argv) == 1:
        exp = ExperimentWiki()
        exp.main()
    else:
        task = sys.argv[1]
        if task == "preprocess":
            preprocess()
        else:
            print 'task {} not supported'.format(task)


if __name__ == '__main__':
    main()


