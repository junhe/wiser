import datetime
import os
from multiprocessing import Pool

from pyreuse import helpers

from utils.utils import LineDocPool, QueryPool
from utils.expbase import Experiment
from .es_bench import WikiClient



BENCH_EXE = "RediSearchBenchmark"
# WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract1.xml"
WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml"
SERVER_PATH = "/users/jhe/workdir/redis-4.0.2/src/redis-server"
REDISEARCH_SO = "/users/jhe/workdir/RediSearch/src/redisearch.so"


def hosts_string(host, n, start_port):
    hosts = [host + ":" + str(start_port+i) for i in range(n)]
    hosts = ",".join(hosts)
    return hosts



class ExperimentRsbenchGo(Experiment):
    def __init__(self):
        self._exp_name = "es-gobench-5shards-001"

        self.paras = helpers.parameter_combinations({
                    'worker_count': [1, 16, 32, 64, 128],
                    'query': ['hello', 'barack obama'],
                    'engine': ['elastic'],
                    'n_shards': [5],
                    'n_hosts': [1]
                    })
        self._n_treatments = len(self.paras)


    def build_index(self, n_shards, n_hosts, engine, start_port):
        helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
                "-hosts \"{hosts}\" -file {filepath}".format(
                    engine=engine, bench_exe=BENCH_EXE, n_shards=n_shards,
                    hosts=hosts_string(self.host, n_hosts, start_port), filepath=WIKI_ABSTRACT))

    def conf(self, i):
        para = self.paras[i]

        d = {
                'n_clients': para['worker_count'],
                'expname': self._exp_name,
                'n_shards': para['n_shards'],
                'n_hosts': para['n_hosts'],
                'query': para['query'],
                'engine': para['engine'],
                'benchmark': 'GoBench',
                }

        if para['engine'] == "elastic":
            es_client = WikiClient("wik")
            d['n_shards'] = es_client.get_number_of_shards()

        return d

    def before(self):
        # self.build_index(self.n_shards, self.n_hosts, self.engine, self.start_port)
        pass

    def treatment(self, conf):
        if conf['engine'] == "elastic":
            start_port = 9200
            n_hosts = 1
            host = "http://localhost"
        elif conf['engine'] == "redis":
            start_port = 6379
            host = "localhost"

        helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
                "-hosts \"{hosts}\" "\
                "-benchmark search -queries \"{query}\" -c {n_clients} " \
                "-o {outpath}".format(
                    engine = conf['engine'],
                    bench_exe = BENCH_EXE,
                    n_clients = conf['n_clients'],
                    hosts = hosts_string(host, conf['n_hosts'], start_port),
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







