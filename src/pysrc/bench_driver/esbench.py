import pprint
import datetime

from elasticsearch import Elasticsearch

from expbase import Experiment
from utils.utils import LineDocPool, QueryPool

class QueryPoolOLD(object):
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


class WikiClient(object):
    def __init__(self):
        self.es_client = Elasticsearch()
        self.index_name = "wiki2"

    def search(self, query_string):
        body = {
            "_source": False,
            # "size": 0, # setting this to 0  will get 5x speedup
            "query": {
                "query_string" : {
                    "fields" : ["body"],
                    "query" : query_string
                }
            }
        }

        response = self.es_client.search(
            index=self.index_name,
            body=body
        )

        return response

    def build_index(self, line_doc_path, n_docs):
        line_pool = LineDocPool(line_doc_path)

        for i, d in enumerate(line_pool.doc_iterator()):
            del d['docdate']

            if i == n_docs:
                break

            res = self.es_client.index(index=self.index_name, doc_type='articles', id=i, body=d)
            print res

            if i % 100 == 0:
                print i,
        print


class ExperimentEs(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "es-experiment"

        exit(1)

    def conf(self, i):
        return {'n_query': 1000}

    def before(self):
        self.client = WikiClient()
        self.query_pool =  QueryPool("/mnt/ssd/downloads/wiki_QueryLog", 10000)

    def beforeEach(self, conf):
        self.start_time = datetime.datetime.now()

    def treatment(self, conf):
        for i in range(conf['n_query']):
            query = self.query_pool.next_query()
            response = self.client.search(query)
            # print query
            # for hit in response['hits']['hits']:
                # print hit['_source']['title']
                # print
                # break

    def afterEach(self, conf):
        self.end_time = datetime.datetime.now()
        total_queries = conf['n_query']
        total_secs = (self.end_time - self.start_time).total_seconds()
        print 'total_queries', total_queries
        print 'total_secs', total_secs
        print 'queries_per_sec', total_queries / total_secs



def main():
    exp = ExperimentEs()
    exp.main()


if __name__ == '__main__':
    main()

