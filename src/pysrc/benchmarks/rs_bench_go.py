import datetime
import time
import os
import signal

from multiprocessing import Pool
import subprocess

from pyreuse import helpers
from pyreuse.helpers import *

from utils.utils import LineDocPool, QueryPool
from utils.expbase import Experiment
from .Clients import ElasticSearchClient
from utils.utils import shcmd


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

def start_elastic(mem_size):
    shcmd('sudo /users/kanwu/results/elastic_run.sh')
    p_elastic = subprocess.Popen('python /users/kanwu/flashsearch/scripts/start_elasticsearch.py ' + str(mem_size), subprocess.PIPE,
                       shell=True, preexec_fn=os.setsid)
    time.sleep(30)
    return p_elastic

def kill_blktrace_on_bg():
    shcmd('pkill blkparse', ignore_error=True)
    shcmd('pkill blktrace', ignore_error=True)
    shcmd('sync')

def start_blktrace_on_bg(dev, resultpath, trace_filter=None):
    prepare_dir_for_path(resultpath)
    # cmd = "sudo blktrace -a write -a read -d {dev} -o - | blkparse -i - > "\
    # cmd = "sudo blktrace -a queue -d {dev} -o - | blkparse -a queue -i - > "\

    if trace_filter is None:
        # trace_filter = '-a issue'
        trace_filter = ''
    else:
        trace_filter = ' '.join(['-a ' + mask for mask in trace_filter])

    cmd = "sudo blktrace {filtermask} -d {dev} -o - | "\
            "blkparse {filtermask} -i - >> "\
        "{resultpath}".format(dev = dev, resultpath = resultpath,
        filtermask = trace_filter)
    print cmd
    p = subprocess.Popen(cmd, shell=True)
    time.sleep(0.3) # wait to see if there's any immediate error.

    if p.poll() != None:
        raise RuntimeError("tracing failed to start")

    return p

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
                    'mem_limit': [4,5,6,7,8,9,10,11,12,13,14,15,16,32],
                    #'n_hosts': [1],
                    #'rebuild_index': [True]
                    'rebuild_index': [False],
                    'warm_engine': [False]
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
                'mem_limit': para['mem_limit'],
                'benchmark': 'GoBench',
                'subexp_index': i,
                'rebuild_index': para['rebuild_index'],
                'warm_engine': para['warm_engine']
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
        #start elasticsearch and blktrace if testing elasticsearch
        if conf["engine"] == "elastic":
            # clear page cache etc.
            self.elastic_process = start_elastic(conf['mem_limit'])
            # start blk trace
            if conf['warm_engine'] is True:
                self.treatment(conf)
                shcmd("mv " + os.path.join(self._subexpdir, "out.csv") + " " + os.path.join(self._subexpdir, "warmup.csv"))
            self.blk_trace = start_blktrace_on_bg('/dev/sdc', os.path.join(self._subexpdir, "blktrace.output"))


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
                        #querypath = '/mnt/ssd/downloads/wiki_QueryLog',
                        querypath = '/users/kanwu/querylog_no_repeated',   # be sure not on /dev/sdc
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
        # print blktrace
        kill_blktrace_on_bg()
        # kill elasticsaerch
        os.killpg(os.getpgid(self.elastic_process.pid), signal.SIGTERM)

def main():
    exp = ExperimentRsbenchGo()
    exp.main()


if __name__ == "__main__":
    main()







