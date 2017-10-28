import datetime
import os

from redisearch import Client, TextField, NumericField, Query
from pyreuse import helpers

from utils.utils import LineDocPool, QueryPool
from utils.expbase import Experiment


class RedisIndex(object):
    def __init__(self, line_doc_path, n_docs):
        # self.line_pool = LineDocPool("/mnt/ssd/downloads/linedoc_tokenized")
        self.line_pool = LineDocPool(line_doc_path)
        self.n_docs = n_docs

    def create_index(self):
        client = Client('wiki')
        client.drop_index()
        client.create_index([TextField('title', weight=5.0), TextField('body')])

        for i, d in enumerate(self.line_pool.doc_iterator()):
            client.add_document(i, title = d['doctitle'], body = d['body'])

            if i + 1 == self.n_docs:
                break

            if i % 100 == 0:
                print i,
        print

        self.client = client

    def search(self, query):
        res = self.client.search(query)
        # print res.total # "1"
        return res



class ExperimentRedis(Experiment):
    def __init__(self):
        self._n_treatments = 3
        self._exp_name = "redis-exp-003"

    def conf(self, i):
        return {'doc_count': 10**(i+3),
                'query_count': 100000
                }

    def setup_engine(self, conf):
        self.index = RedisIndex("/mnt/ssd/downloads/linedoc_tokenized", conf['doc_count'])
        self.index.create_index()

    def beforeEach(self, conf):
        self.query_pool = QueryPool("/mnt/ssd/downloads/wiki_QueryLog", conf['query_count'])
        self.setup_engine(conf)
        # self.engine.index.display()

        self.starttime = datetime.datetime.now()

    def treatment(self, conf):
        for i in range(conf['query_count']):
            query = self.query_pool.next_query()
            doc_ids = self.index.search(query)

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


