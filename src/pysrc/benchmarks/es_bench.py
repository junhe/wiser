import pprint
import os
import datetime
import time
from multiprocessing import Pool

from elasticsearch import Elasticsearch

from expbase import Experiment
from utils.utils import LineDocPool, QueryPool, index_wikiabs_on_elasticsearch
from pyreuse import helpers


class WikiClient(object):
    def __init__(self, index_name):
        self.es_client = Elasticsearch()
        self.index_name = index_name

    def search(self, query_string):
        body = {
            "_source": False,
            "size": 0, # setting this to 0  will get 5x speedup
            "query": {
                "query_string" : {
                    # "fields" : ["body"],
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

    def get(self):
        return self.es_client.indices.get(self.index_name)

    def get_number_of_shards(self):
        d = self.get()
        return d[self.index_name]['settings']['index']['number_of_shards']

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



def worker_wrapper(args):
    worker(*args)


def worker(query_pool, query_count):
    client = WikiClient("wik")

    for i in range(query_count):
        query = query_pool.next_query()
        ret = client.search(query)
        if i % 5000 == 0:
            print os.getpid(), "{}/{}".format(i, query_count)


class ExperimentEs(Experiment):
    def __init__(self):
        self._exp_name = "es-pybench-5shards-003"
        self.wiki_abstract_path = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml"

        self.paras = helpers.parameter_combinations({
                    'worker_count': [1, 16, 32, 64, 128],
                    'query': ['hello', 'barack obama', 'wiki-query-log'],
                    'engine': ['elastic']
                    })
        self._n_treatments = len(self.paras)

        self.client = WikiClient("wik")

    def conf(self, i):
        para = self.paras[i]

        d = {
                'doc_count': 10**(i+3),
                'query_count': int(50000 / para['worker_count']),
                'query': para['query'],
                'engine': para['engine'],
                'n_shards': self.client.get_number_of_shards(),
                "benchmark": "PyBench",
                'n_clients': para['worker_count']
                }

        if para['query'] == 'wiki-query-log':
            d['query_source'] = "/mnt/ssd/downloads/wiki_QueryLog"
        else:
            d['query_source'] = [para['query']]

        return d

    def before(self):
        # index_wikiabs_on_elasticsearch(self.wiki_abstract_path)
        pass

    def beforeEach(self, conf):
        # self.client = WikiClient("wik")
        # self.client.delete_index()
        # self.client.build_index("/mnt/ssd/downloads/linedoc_tokenized", conf['doc_count'])
        # self.client.clear_cache()
        # time.sleep(5)

        print "Query Source", conf['query_source']
        self.query_pool =  QueryPool(conf['query_source'], conf['query_count'])

        self.starttime = datetime.datetime.now()

    def treatment(self, conf):
        p = Pool(conf['n_clients'])
        p.map(worker_wrapper, [
            (self.query_pool, conf['query_count']) for _ in range(conf['n_clients'])
            ])

    def afterEach(self, conf):
        self.endtime = datetime.datetime.now()
        duration = (self.endtime - self.starttime).total_seconds()
        print "Duration:", duration
        query_per_sec = conf['n_clients'] * conf['query_count'] / duration
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
        helpers.dump_json(conf, config_path)



def main():
    exp = ExperimentEs()
    exp.main()


if __name__ == '__main__':
    main()

