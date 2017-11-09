import os
import shlex
import subprocess
import time

from pyreuse.apputils.flamegraph import *
# from flamegraph import *
from pyreuse.helpers import *

REDISEARCH_SO = "/users/jhe/workdir/RediSearch/src/redisearch.so"


def main():
    fgraph = FlameGraph("/tmp/FlameGraph", "./perf_workdir")

    redis_server_cmd = "/users/jhe/workdir/redis-4.0.2/src/redis-server --dir /mnt/ssd/ "\
        "--dbfilename redis-wiki-abs.db --loadmodule {mod}".format(mod=REDISEARCH_SO)
    redis_server_subp = subprocess.Popen(shlex.split(redis_server_cmd))

    time.sleep(1)
    time.sleep(40) # wait until all data loaded
    perf_subp = fgraph.attach(pid=redis_server_subp.pid)

    time.sleep(1)
    cmd = "/users/jhe/workdir/go/bin/RediSearchBenchmark "\
            "-engine redis -shards 1 -hosts \"localhost:6379\" "\
            "-duration 60 "\
            "-benchmark search -queries \"hello\" -c 1 -o /tmp/out.csv"
    print cmd
    subprocess.call(shlex.split(cmd))

    time.sleep(1)

    fgraph.detach(perf_subp)
    redis_server_subp.terminate()
    redis_server_subp.wait()

    fgraph.produce_graph("flame.svg")

    shcmd("cp ./perf_workdir/flame.svg /tmp/results/flame/")

if __name__ == "__main__":
    main()

