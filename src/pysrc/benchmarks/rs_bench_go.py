
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
from pyreuse.sysutils.blocktrace import *
from pyreuse.sysutils.ncq import *
from pyreuse.apputils.flamegraph import *

BENCH_EXE = "RediSearchBenchmark"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml_1"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki-20171020-abstract.xml"
#WIKI_ABSTRACT = "/mnt/ssd/downloads/test.xml"

#Full Wiki: 
WIKI_ABSTRACT = "/mnt/ssd/downloads/enwiki.linedoc.xml_clean"

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

    if engine == 'elastic':
        print 'prepare to merge'
        cmd = "curl -XPOST \'localhost:9200/wik/_forcemerge?max_num_segments=1&pretty\'"
        shcmd(cmd)

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
    time.sleep(50)
    return p_elastic

def kill_blktrace_on_bg():
    shcmd('pkill blkparse', ignore_error=True)
    shcmd('pkill blktrace', ignore_error=True)
    shcmd('sync')

def kill_ps_on_bg():
    shcmd('pkill ps', ignore_error=True)
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

def analyze_ncq(subdir):
    table = parse_ncq(event_path = os.path.join(subdir, 'blkparse-output.txt.parsed'))
    with open(os.path.join(subdir, 'ncq.txt'), "w") as ncq_output:
        for each_line in table:
            for k,v in each_line.items():
                ncq_output.write(str(v) + ' ')
            ncq_output.write("\n")


class ExperimentRsbenchGo(Experiment):
    """
    To run the experiment, you need to start Redis server manually first, by
    python -m benchmarks.start_redis_server n_hosts

    Then you can execute this experiment by
    make rs_bench_go
    """
    def __init__(self):
        self._exp_name = "elastc_io"

        self.paras = helpers.parameter_combinations({
                    #'worker_count': [1],
                    'worker_count': [16],
                    'query': ['wiki-query-log'],
                    #'query': ['tugman'],
                    #'query': ['hello'],
                    #'query': ['garnieria'], #'ripdo', 'ripdo liftech'],
                    #'query': ['from', 'hello', 'ripdo', 'from also', 'hello world', 'ripdo liftech'], #'hello', 'from', 'ripdo', 'from also', 'hello world', 'ripdo liftech', 'hello', 'from', 'ripdo', 'from also', 'hello world', 'ripdo liftech'], # 'barack obama', 'unit state america', 'wiki-query-log'],
                    #'query': ['hello world'],
                    #'query': ['hello world', 'barack obama', 'from also'],
                    'engine': ['elastic'],
                    #'engine': ['redis'],
                    'n_shards': [1],
                    'n_hosts': [1],
                    #'mem_limit': [3,4,5,6,7, 8, 32,64],
                    'mem_limit': [128],
                    #'readahead': [4, 8, 16, 32, 64],
                    #'readahead': [0, 4, 8, 16, 32, 64, 128],
                    'readahead': [128],
                    #'n_hosts': [1],
                    'rebuild_index': [False],
                    #'rebuild_index': [False],
                    #'warm_engine': [True],
                    'warm_engine': [False],
                    'single_query': [False]         # whether we are just running one request, then we need to warm up with ripdo liftech
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
                'readahead': para['readahead'],
                'benchmark': 'GoBench',
                'subexp_index': i,
                'rebuild_index': para['rebuild_index'],
                'warm_engine': para['warm_engine'],
                'single_query': para['single_query']
                }
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
            #empty the log
            shcmd('echo > /tmp/results/lucene.log')

            # set readahead block
            cmd = 'echo ' + str(conf['readahead']) + ' > /sys/block/sdc/queue/read_ahead_kb'
            shcmd(cmd)

            # clear page cache + start elasticsearch + sleep etc.
            self.elastic_process = start_elastic(conf['mem_limit'])

            # warm up, if needed
            # run some useless queries before
            if conf['single_query'] is True:
                #warm with ripdo liftech
                tmp = conf['query']
                conf['query'] = 'ripdo liftech'
                self.treatment(conf)
                self.treatment(conf)
                self.treatment(conf)
                self.treatment(conf)
                self.treatment(conf)
                shcmd('sync')
                shcmd("mv " + os.path.join(self._subexpdir, "out.csv") + " " + os.path.join(self._subexpdir, "warmup.csv"))
                conf['query'] = tmp
               
                #cmd = 'sync; echo 3 > /proc/sys/vm/drop_caches'
                #shcmd(cmd) 
            # warm up
            if conf['warm_engine'] is True:
                self.treatment(conf)
                #self.treatment(conf)
                #self.treatment(conf)
                #self.treatment(conf)
                shcmd("mv " + os.path.join(self._subexpdir, "out.csv") + " " + os.path.join(self._subexpdir, "warmup.csv"))
            


            # start perf to generate flamegraph
            #self.fgraph = FlameGraph('/users/kanwu/FlameGraph', '/users/kanwu/FlameGraph')
            #self.perf_p = self.fgraph.attach(self.elastic_process.pid, 99)
            

            # start blktrace
            trace_filter=['issue']#, 'complete']
            self.blk_trace = start_blktrace_on_bg('/dev/sdc', os.path.join(self._subexpdir, "blktrace.output"), trace_filter=trace_filter)
            # start iostat
            shcmd("iostat -x sdc 1 300 > " + os.path.join(self._subexpdir, "iostat.out") + " &")
            # start ps to see page faults overtime

            # log page faults during starting Elasticsearch
            elastic_pid = subprocess.check_output(['pidof', 'java']).strip('\n')
            shcmd('ps -o maj_flt ' + elastic_pid + ' >> ' + os.path.join(self._subexpdir, 'page_faults.out'))
            
            # empty the log
            shcmd('echo > /tmp/results/lucene.log')
            shcmd('sync')

    def treatment(self, conf):
        if conf['query'] == 'wiki-query-log':
            helpers.shcmd("{bench_exe} -engine {engine} -shards {n_shards} " \
                    "-hosts \"{hosts}\" "\
                    "-benchmark search -querypath \"{querypath}\" -c {n_clients} " \
                    "-o {outpath} ".format(
                        engine = conf['engine'],
                        bench_exe = BENCH_EXE,
                        n_clients = conf['n_clients'],
                        hosts = hosts_string(conf['host'], conf['n_hosts'], conf['start_port']),
                        n_shards = conf['n_shards'],
                        #querypath = '/mnt/ssd/downloads/wiki_QueryLog',
                        #querypath = '/users/kanwu/test_data/querylog_no_repeated',   # be sure not on /dev/sdc
                        querypath = '/users/kanwu/test_data/realistic_with_phrases',   # be sure not on /dev/sdc
                        #querypath = '/users/kanwu/test_data/querylog_realistic',   # be sure not on /dev/sdc
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
        if conf["engine"] == "elastic":
            
            # kill ps
            elastic_pid = subprocess.check_output(['pidof', 'java']).strip('\n')
            shcmd('ps -o maj_flt ' + elastic_pid + ' >> ' + os.path.join(self._subexpdir, 'page_faults.out'))
            
            # kill blktrace
            kill_blktrace_on_bg()
           
            # generate flamegraph
            #self.fgraph.detach(self.perf_p)
            #self.fgraph.produce_graph(to_path='/tmp/flamegraph.svg')
            
            # kill elasticsaerch
            shcmd('sync')
            time.sleep(10)
            elastic_pid = os.getpgid(self.elastic_process.pid)
            os.killpg(elastic_pid, signal.SIGTERM)
        
            # parse blkparse results
            blkresult = BlktraceResultInMem(
                    sector_size=512,
                    event_file_column_names=['pid', 'action', 'operation', 'offset', 'size',
                        'timestamp', 'pre_wait_time', 'sync'],
                    raw_blkparse_file_path=os.path.join(self._subexpdir, "blktrace.output"),
                    parsed_output_path=(os.path.join(self._subexpdir, 'blkparse-output.txt.parsed')))
            blkresult.create_event_file()
            # parse ncq depth:
            analyze_ncq(self._subexpdir)

            # mv lucene log to specific file
            shcmd('sync')
            lucene_log_dir = os.path.join(self._subexpdir, 'lucene.log')
            shcmd('cp /tmp/results/lucene.log ' + lucene_log_dir)

def main():
    exp = ExperimentRsbenchGo()
    exp.main()


if __name__ == "__main__":
    main()







