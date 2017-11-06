import pprint
import os
import datetime
import time
from multiprocessing import Pool

from elasticsearch import Elasticsearch
from redisearch import Client, TextField, NumericField, Query

from expbase import Experiment
from utils.utils import LineDocPool, QueryPool, index_wikiabs_on_elasticsearch
from pyreuse import helpers


class RedisClient(object):
    def __init__(self, index_name):
        self.client = Client(index_name)

    def build_index(self, line_doc_path, n_docs):
        line_pool = LineDocPool(line_doc_path)

        try:
            self.client.drop_index()
        except:
            pass

        self.client.create_index([TextField('title'), TextField('url'), TextField('body')])

        for i, d in enumerate(line_pool.doc_iterator()):
            self.client.add_document(i, nosave = True, title = d['doctitle'],
                    url = d['url'], body = d['body'])

            if i + 1 == n_docs:
                break

            if i % 1000 == 0:
                print "{}/{} building index".format(i, n_docs)

    def search(self, query):
        q = Query(query).paging(0, 5).verbatim()
        res = self.client.search(q)
        # print res.total # "1"
        return res


class ElasticSearchClient(object):
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


def worker(query_pool, query_count, engine):
    client = create_client(engine)

    for i in range(query_count):
        query = query_pool.next_query()
        ret = client.search(query)
        if i % 5000 == 0:
            print os.getpid(), "{}/{}".format(i, query_count)


def create_client(engine):
    if engine == "elastic":
        return ElasticSearchClient("wik")
    elif engine == "redis":
        return RedisClient("wik")


class ExperimentEsRs(Experiment):
    def __init__(self):
        self._exp_name = "redis-pybench-1shard-010"
        self.wiki_abstract_path = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml"

        parameters = {
                    'worker_count': [1, 16, 32, 64, 128],
                    'query': ['hello', 'barack obama', 'wiki-query-log'],
                    'engine': ['redis'],
                    'line_doc_path': ["/mnt/ssd/downloads/enwiki-abstract.linedoc.withurl"],
                    'n_shards': [1],
                    'rebuild_index': [True]
                    }
        self.paras = helpers.parameter_combinations(parameters)
        self._n_treatments = len(self.paras)
        assert len(parameters['engine']) == 1, \
            "Allow only 1 engine because we only build index for one engine at an experiment"

    def conf(self, i):
        para = self.paras[i]

        conf = {
                'doc_count': 10**8,
                'query_count': int(50000 / para['worker_count']),
                'query': para['query'],
                'engine': para['engine'],
                "benchmark": "PyBench",
                "line_doc_path": para['line_doc_path'],
                'n_clients': para['worker_count'],
                'rebuild_index': para['rebuild_index']
                }

        if para['query'] == 'wiki-query-log':
            conf['query_source'] = "/mnt/ssd/downloads/wiki_QueryLog"
        else:
            conf['query_source'] = [para['query']]

        if conf['engine'] == 'elastic':
            client = create_client(conf['engine'])
            conf['n_shards'] = client.get_number_of_shards(),
        elif conf['engine'] == 'redis':
            conf['n_shards'] = para['n_shards']

        if conf['engine'] == 'redis':
            assert conf['n_shards'] == 1, "pybench only supports one redis shard at this time"

        return conf

    def before(self):
        conf = self.conf(0)

        # build index
        if conf['rebuild_index'] is True:
            client = create_client(conf['engine'])
            client.build_index(conf['line_doc_path'], conf['doc_count'])

    def beforeEach(self, conf):
        print "Query Source", conf['query_source']
        self.query_pool =  QueryPool(conf['query_source'], conf['query_count'])

        self.starttime = datetime.datetime.now()

    def treatment(self, conf):
        p = Pool(conf['n_clients'])
        p.map(worker_wrapper, [
            (self.query_pool, conf['query_count'], conf['engine']) for _ in range(conf['n_clients'])
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
    exp = ExperimentEsRs()
    exp.main()


if __name__ == '__main__':
    main()

