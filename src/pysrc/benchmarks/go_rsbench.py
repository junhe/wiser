import datetime
import os
from multiprocessing import Pool

from pyreuse import helpers

from utils.utils import LineDocPool, QueryPool
from utils.expbase import Experiment



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
        self._n_treatments = 5
        self._exp_name = "rsbench-go-ES-hello-simpler-query-03"

        self.n_shards = 5
        self.n_hosts = 1
        self.host = "http://localhost"

        self.engine = "elastic"

        if self.engine == "elastic":
            self.start_port = 9200
            self.n_hosts = 1
        elif self.engine == "redis":
            self.start_port = 6379


    def build_index(self, n_shards, n_hosts, engine, start_port):
        helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
                "-hosts \"{hosts}\" -file {filepath}".format(
                    engine=engine, bench_exe=BENCH_EXE, n_shards=n_shards,
                    hosts=hosts_string(self.host, n_hosts, start_port), filepath=WIKI_ABSTRACT))

    def conf(self, i):
        n_clients = [1, 16, 32, 64, 128]
        return {'n_clients': n_clients[i],
                'expname': self._exp_name,
                'n_shards': self.n_shards,
                'n_hosts': self.n_hosts,
                'query': 'hello',
                'engine': self.engine,
                'note': 'simpler-es'
                }

    def before(self):
        # self.build_index(self.n_shards, self.n_hosts, self.engine, self.start_port)
        pass

    def treatment(self, conf):
        helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
                "-hosts \"{hosts}\" "\
                "-benchmark search -queries \"{query}\" -c {n_clients} " \
                "-o {outpath}".format(
                    engine = self.engine,
                    bench_exe = BENCH_EXE,
                    n_clients = conf['n_clients'],
                    hosts = hosts_string(self.host, self.n_hosts, self.start_port),
                    n_shards = self.n_shards,
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







