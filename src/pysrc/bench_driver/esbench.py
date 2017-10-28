import pprint
import os
import datetime
import time

from elasticsearch import Elasticsearch

from expbase import Experiment
from utils.utils import LineDocPool, QueryPool
from pyreuse import helpers

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
            "size": 0, # setting this to 0  will get 5x speedup
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

    def delete_index(self):
        ret = self.es_client.indices.delete(index=self.index_name)
        assert ret['acknowledged'] == True

    def clear_cache(self):
        ret = self.es_client.indices.clear_cache(index=self.index_name)

    def build_index(self, line_doc_path, n_docs):
        line_pool = LineDocPool(line_doc_path)

        for i, d in enumerate(line_pool.doc_iterator()):
            del d['docdate']

            if i == n_docs:
                break

            res = self.es_client.index(index=self.index_name, doc_type='articles', id=i, body=d)

            if i % 100 == 0:
                print "{}/{}".format(i, n_docs)
        print


class ExperimentEs(Experiment):
    def __init__(self):
        self._n_treatments = 3
        self._exp_name = "es-fast"

    def conf(self, i):
        return {
                'doc_count': 10**(i+3),
                'query_count': 100000}

    def before(self):
        pass

    def beforeEach(self, conf):
        self.client = WikiClient()
        self.client.delete_index()
        self.client.build_index("/mnt/ssd/downloads/linedoc_tokenized", conf['doc_count'])
        self.client.clear_cache()
        time.sleep(5)

        self.query_pool =  QueryPool("/mnt/ssd/downloads/wiki_QueryLog", conf['query_count'])

        self.starttime = datetime.datetime.now()

    def treatment(self, conf):
        for i in range(conf['query_count']):
            query = self.query_pool.next_query()
            response = self.client.search(query)
            # print query
            # print response['hits']['total']
            # for hit in response['hits']['hits']:
                # print hit['_source']['title']
                # print
                # break

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
    exp = ExperimentEs()
    exp.main()


if __name__ == '__main__':
    main()

