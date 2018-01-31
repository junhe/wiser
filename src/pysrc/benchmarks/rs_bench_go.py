import datetime
import os
from multiprocessing import Pool
import subprocess

from pyreuse import helpers

from utils.utils import LineDocPool, QueryPool
from utils.expbase import Experiment
from .Clients import ElasticSearchClient



BENCH_EXE = "RediSearchBenchmark"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml_1"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/test.xml"
WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki.linedoc.xml"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-latest-pages-articles.xml"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-latest-pages-articles1.xml"
# WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-latest-abstract18.xml"
# SERVER_PATH = "/users/jhe/redis-4.0.2/src/redis-server"
# REDISEARCH_SO = "/users/jhe/RediSearch/src/redisearch.so"
SERVER_PATH = "/users/kanwu/redis-4.0.2/src/redis-server"
REDISEARCH_SO = "/users/kanwu/RediSearch/src/redisearch.so"


def build_index(n_shards, n_hosts, engine, start_port, host):
    helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
            "-hosts \"{hosts}\" -file {filepath}".format(
                engine=engine, bench_exe=BENCH_EXE, n_shards=n_shards,
                hosts=hosts_string(host, n_hosts, start_port), filepath=WIKI_ABSTRACT))


def hosts_string(host, n, start_port):
    hosts = [host + ":" + str(start_port+i) for i in range(n)]
    hosts = ",".join(hosts)
    return hosts


def start_redis(port):
    cmd = "{server} --port {port} --loadmodule {mod}".format(
    # cmd = "gdb --args {server} --port {port} --loadmodule {mod}".format(
            server=SERVER_PATH, mod=REDISEARCH_SO, port=port)
    p = subprocess.Popen(cmd, shell = True)
    return p

def update_address(conf):
    if conf['engine'] == "elastic":
        conf['start_port'] = 9200
        conf['host'] = "http://localhost"
    elif conf['engine'] == "redis":
        conf['start_port'] = 6379
        conf['host'] = "localhost"


class ExperimentRsbenchGo(Experiment):
    """
    To run the experiment, you need to start Redis server manually first, by
    python -m benchmarks.start_redis_server n_hosts

    Then you can execute this experiment by
    make rs_bench_go
    """
    def __init__(self):
        self._exp_name = "test"

        self.paras = helpers.parameter_combinations({
                    #'worker_count': [1, 16, 32, 64, 128],
                    'worker_count': [1],
                    'query': ['wiki-query-log'],
                    #'query': ['hello', 'barack obama', 'unit state america', 'wiki-query-log'],
                    #'query': ['unit state america', 'wiki-query-log'],
                    'engine': ['elastic'],
                    #'engine': ['redis'],
                    'n_shards': [1],
                    'n_hosts': [1],
                    #'n_hosts': [1],
                    #'rebuild_index': [True]
                    'rebuild_index': [False]
                    })
        self._n_treatments = len(self.paras)

    def conf(self, i):
        para = self.paras[i]

        conf = {
                'n_clients': para['worker_count'],
                'expname': self._exp_name,
                'n_shards': para['n_shards'],
                'n_hosts': para['n_hosts'],
                'query': para['query'],
                'engine': para['engine'],
                'benchmark': 'GoBench',
                'subexp_index': i,
                'rebuild_index': para['rebuild_index']
                }
        '''
        if para['engine'] == "elastic":
            es_client = ElasticSearchClient("wiki")   #TODO?
            conf['n_shards'] = es_client.get_number_of_shards()
        '''
        update_address(conf)
        return conf

    def beforeEach(self, conf):
	print conf
        if conf["engine"] == "redis" and not helpers.is_command_running("redis-4.0.2/src/redis-server"):
            raise RuntimeError("You have to have redis_server running")
        
        if conf['subexp_index'] == 0 and conf['rebuild_index'] is True:
            build_index(conf['n_shards'], conf['n_hosts'], conf['engine'], conf['start_port'], conf['host'])
            '''
            if conf['engine'] == "elastic":
                es_client = ElasticSearchClient("wik")   #TODO?
                conf['n_shards'] = es_client.get_number_of_shards()
            '''

    def treatment(self, conf):
        if conf['query'] == 'wiki-query-log':
            helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
                    "-hosts \"{hosts}\" "\
                    "-benchmark search -querypath \"{querypath}\" -c {n_clients} " \
                    "-o {outpath}".format(
                        engine = conf['engine'],
                        bench_exe = BENCH_EXE,
                        n_clients = conf['n_clients'],
                        hosts = hosts_string(conf['host'], conf['n_hosts'], conf['start_port']),
                        n_shards = conf['n_shards'],
                        querypath = '/mnt/ssd/downloads/wiki_QueryLog',
                        outpath = os.path.join(self._subexpdir, "out.csv")
                        ))
        else:
            helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
                    "-hosts \"{hosts}\" "\
                    "-benchmark search -queries \"{query}\" -c {n_clients} " \
                    "-o {outpath}".format(
                        engine = conf['engine'],
                        bench_exe = BENCH_EXE,
                        n_clients = conf['n_clients'],
                        hosts = hosts_string(conf['host'], conf['n_hosts'], conf['start_port']),
                        n_shards = conf['n_shards'],
                        query = conf['query'],
                        outpath = os.path.join(self._subexpdir, "out.csv")
                        ))

    def afterEach(self, conf):
        helpers.dump_json(conf, os.path.join(self._subexpdir, "config.json"))


def main():
    exp = ExperimentRsbenchGo()
    exp.main()


if __name__ == "__main__":
    main()







