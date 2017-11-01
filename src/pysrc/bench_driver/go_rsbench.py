import datetime
import os
from multiprocessing import Pool

from pyreuse import helpers

from utils.utils import LineDocPool, QueryPool
from utils.expbase import Experiment



BENCH_EXE = "/users/jhe/workdir/RediSearchBenchmark/RediSearchBenchmark"
# WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract1.xml"
WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml"
SERVER_PATH = "/users/jhe/workdir/redis-4.0.2/src/redis-server"
REDISEARCH_SO = "/users/jhe/workdir/RediSearch/src/redisearch.so"


def hosts_string(n):
    hosts = ["localhost:" + str(6379+i) for i in range(n)]
    hosts = ",".join(hosts)
    return hosts

class ExperimentRsbenchGo(Experiment):
    def __init__(self):
        self._n_treatments = 5
        self._exp_name = "rsbench-go-5-shards-hello-01"

        self.n_shards = 5
        self.n_hosts = self.n_shards

    def build_index(self, n_shards, n_hosts):
        helpers.shcmd("{bench_exe} -engine redis -shards {n_shards} " \
                "-hosts \"{hosts}\" -file {filepath}".format(
                    bench_exe=BENCH_EXE, n_shards=n_shards,
                    hosts=hosts_string(n_hosts), filepath=WIKI_ABSTRACT))

    def conf(self, i):
        n_clients = [1, 16, 32, 64, 128]
        return {'n_clients': n_clients[i],
                'expname': self._exp_name,
                'n_shards': self.n_shards,
                'n_hosts': self.n_hosts,
                'query': 'hello'
                }

    def before(self):
        # self.build_index(self.n_shards, self.n_hosts)
        pass

    def treatment(self, conf):
        helpers.shcmd("{bench_exe} -engine redis -shards {n_shards} " \
                "-hosts \"{hosts}\" "\
                "-benchmark search -queries \"{query}\" -c {n_clients} " \
                "-o {outpath}".format(
                    bench_exe = BENCH_EXE, n_clients = conf['n_clients'],
                    hosts = hosts_string(self.n_hosts), n_shards = self.n_shards,
                    query = conf['query'],
                    outpath = os.path.join(self._subexpdir, "out.csv")))

    def afterEach(self, conf):
        helpers.dump_json(conf, os.path.join(self._subexpdir, "config.json"))


def main():
    exp = ExperimentRsbenchGo()
    exp.main()


if __name__ == "__main__":
    main()







