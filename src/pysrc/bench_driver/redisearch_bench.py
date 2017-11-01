import datetime
import os
from multiprocessing import Pool

from redisearch import Client, TextField, NumericField, Query
from pyreuse import helpers

from utils.utils import LineDocPool, QueryPool
from utils.expbase import Experiment


class RedisIndex(object):
    def __init__(self, index_name):
        self.client = Client(index_name)

    def create_index(self, line_doc_path, n_docs):
        line_pool = LineDocPool(line_doc_path)

        try:
            self.client.drop_index()
        except:
            pass

        self.client.create_index([TextField('title', weight=5.0), TextField('body')])

        for i, d in enumerate(line_pool.doc_iterator()):
            self.client.add_document(i, title = d['doctitle'], body = d['body'])

            if i + 1 == n_docs:
                break

            if i % 100 == 0:
                print "{}/{}".format(i, n_docs)


    def search(self, query):
        res = self.client.search(query)
        # print res.total # "1"
        return res


def worker_wrapper(args):
    worker(*args)

def worker(query_pool, query_count):
    index = RedisIndex("wiki")

    for i in range(query_count):
        query = query_pool.next_query()
        query = "barack obama"
        doc_ids = index.search(query)
        if i % 1000 == 0:
            print os.getpid(), "{}/{}".format(i, query_count)

class ExperimentRedis(Experiment):
    def __init__(self):
        self._n_treatments = 3
        self._exp_name = "redis-obama-001"

    def conf(self, i):
        return {'doc_count': 10**(i+3),
                'query_count': 100000,
                'n_workers': 1
                }

    def setup_engine(self, conf):
        self.index = RedisIndex("wiki")
        self.index.create_index("/mnt/ssd/downloads/linedoc_tokenized", conf['doc_count'])

    def beforeEach(self, conf):
        self.query_pool = QueryPool("/mnt/ssd/downloads/wiki_QueryLog", conf['query_count'])
        self.setup_engine(conf)
        # self.engine.index.display()

        self.starttime = datetime.datetime.now()

    def treatment(self, conf):
        # for i in range(conf['query_count']):
            # query = self.query_pool.next_query()
            # doc_ids = self.index.search(query)
        # worker(self.index, self.query_pool, conf['query_count'])

        p = Pool(conf['n_workers'])
        p.map(worker_wrapper, [
            (self.query_pool, conf['query_count']) for _ in range(conf['n_workers'])
            ])
        # p.map(worker_wrapper, [
            # (self.query_pool, 2, 3),
            # (4, 5, 6),
            # ])

    def afterEach(self, conf):
        self.endtime = datetime.datetime.now()
        duration = (self.endtime - self.starttime).total_seconds()
        print "Duration:", duration
        query_per_sec = conf['query_count'] * conf['n_workers'] / duration
        print "Query per second:", query_per_sec
        d = {
            "duration": duration,
            "query_per_sec": query_per_sec,
            }
        d.update(conf)

        perf_path = os.path.join(self._subexpdir, "perf.txt")
        print 'writing to', perf_path
        helpers.table_to_file([d], perf_path, width=0)

        config_path = os.path.join(self._subexpdir, "config.json")
        helpers.shcmd("touch " + config_path)


def main():
    exp = ExperimentRedis()
    exp.main()


if __name__ == "__main__":
    main()


