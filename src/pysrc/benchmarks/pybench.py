import pprint
import redis
import os
import datetime
import time
from multiprocessing import Pool

from elasticsearch import Elasticsearch
from redisearch import Client, TextField, NumericField, Query

from expbase import Experiment
from utils.utils import QueryPool
from pyreuse import helpers

from .Clients import *
from . import rs_bench_go



INDEX_NAME = "wik{0}"

def worker_wrapper(args):
    worker(*args)


def worker(query_pool, query_count, engine):
    client = create_client(engine)

    for i in range(query_count):
        query = query_pool.next_query()
        ret = client.search(query)
        #if i % 5000 == 0:
        #    print os.getpid(), "{}/{}".format(i, query_count)


def create_client(engine):
    if engine == "elastic":
        return ElasticSearchClient(INDEX_NAME)
    elif engine == "redis":
        # return RediSearchClient(INDEX_NAME)
        return RedisClient(INDEX_NAME)


class ExperimentEsRs(Experiment):
    def __init__(self):
        self._exp_name = "redis-pybench-1shard-014"
        self.wiki_abstract_path = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml"

        parameters = {
                    'worker_count': [1, 16, 32, 64, 128],
                    #'query': ['hello', 'barack obama', 'wiki-query-log'],
                    'query': ['wiki-query-log'],
                    #'engine': ['elastic'],
                    'engine': ['redis'],
                    'line_doc_path': ["/mnt/ssd/downloads/enwiki-abstract.linedoc.withurl"],
                    'n_shards': [1],
                    'n_hosts': [1],
                    'rebuild_index': [False],
                    'index_builder': ['RediSearchBenchmark'] # or PyBench
                    }
        self.paras = helpers.parameter_combinations(parameters)
        self._n_treatments = len(self.paras)
        assert len(parameters['engine']) == 1, \
            "Allow only 1 engine because we only build index for one engine at an experiment"

    def conf(self, i):
        para = self.paras[i]

        conf = {
                'doc_count': 10**8,
                'query_count': int(500000 / para['worker_count']),
                'query': para['query'],
                'engine': para['engine'],
                #"benchmark": "PyBench",
                "benchmark": "RediSearchBenchmark",
                "line_doc_path": para['line_doc_path'],
                'n_clients': para['worker_count'],
                'rebuild_index': para['rebuild_index'],
                'index_builder': para['index_builder'],
                'n_hosts': para['n_hosts']
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
            if conf['index_builder'] == "PyBench":
                client = create_client(conf['engine'])
                client.build_index(conf['line_doc_path'], conf['doc_count'])
            elif conf['index_builder'] == "RediSearchBenchmark":
                rs_bench_go.update_address(conf)
                rs_bench_go.build_index(n_shards=conf['n_shards'],
                        n_hosts=conf['n_hosts'],
                        engine=conf['engine'],
                        start_port=conf['start_port'],
                        host=conf['host'])
            else:
                raise NotImplementedError

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

