import os
import shlex
import subprocess
import time

from pyreuse.apputils.flamegraph import *
# from flamegraph import *
from pyreuse.helpers import *

REDISEARCH_SO = "/users/jhe/workdir/redisearch-wisc/src/redisearch.so"
REDIS_CLI = "/users/jhe/workdir/redis/src/redis-cli"


def make(rs_cmd, redis_cmd):
    with cd("/users/jhe/workdir/redisearch-wisc/"):
        shcmd("make distclean && {}".format(rs_cmd))

    with cd("/users/jhe/workdir/redis/"):
        shcmd("make distclean && make".format(redis_cmd))


def make_for_perf():
    make("./compile-debug.sh", "./compile-debug.sh")


def make_default():
    make("make", "make")


def main_no_perf():
    # make_default()
    # make_for_perf()

    redis_server_cmd = "/users/jhe/workdir/redis/src/redis-server --dir /mnt/ssd/ "\
        "--dbfilename redis-wiki-abs.db --loadmodule {mod} SAFEMODE".format(mod=REDISEARCH_SO)
    redis_server_subp = subprocess.Popen(shlex.split(redis_server_cmd))

    time.sleep(40)
    cmd = "/users/jhe/workdir/go/bin/RediSearchBenchmark "\
            "-engine redis -shards 1 -hosts \"localhost:6379\" "\
            "-duration 60 "\
            "-benchmark search -queries \"hello\" -c 32 -o /tmp/out.csv"
    print cmd
    subprocess.call(shlex.split(cmd))

    redis_server_subp.terminate()


def main_debug():
    with cd("/users/jhe/workdir/redisearch-wisc/"):
        shcmd("./compile-gdb.sh")

    redis_server_cmd = "/users/jhe/workdir/redis/src/redis-server --dir /mnt/ssd/ "\
        "--dbfilename redis-wiki-abs-small.db --loadmodule {mod}".format(mod=REDISEARCH_SO)

    print redis_server_cmd
    redis_server_subp = subprocess.Popen(shlex.split(redis_server_cmd))

    time.sleep(100)

    cli_cmd = "{exe} FT.SEARCH wik{{0}} hello LIMIT 0 5  WITHSCORES VERBATIM".format(exe = REDIS_CLI)
    print cli_cmd
    subprocess.call(cli_cmd, shell=True)
    print "COMMAND FINISHED"

    time.sleep(1)

    redis_server_subp.terminate()


def main_gdb():
    with cd("/users/jhe/workdir/redisearch-wisc/"):
        shcmd("make distclean && ./compile-gdb.sh")

    with cd("/users/jhe/workdir/redis/"):
        shcmd("make distclean && ./compile-gdb.sh")

    redis_server_cmd = "gdb --args /users/jhe/workdir/redis/src/redis-server --dir /mnt/ssd/ "\
        "--dbfilename redis-wiki-abs-small.db --loadmodule {mod}".format(mod=REDISEARCH_SO)

    print redis_server_cmd
    redis_server_subp = subprocess.Popen(shlex.split(redis_server_cmd))

    # time.sleep(2)

    # cli_cmd = "{exe} FT.SEARCH wik{{0}} hello LIMIT 0 5  WITHSCORES VERBATIM".format(exe = REDIS_CLI)
    # subprocess.call(cli_cmd, shell=True)

    redis_server_subp.wait()


def main_perf():
    make_for_perf()

    fgraph = FlameGraph("/tmp/FlameGraph", "./perf_workdir")

    redis_server_cmd = "/users/jhe/workdir/redis/src/redis-server --dir /mnt/ssd/ "\
        "--dbfilename redis-wiki-abs.db --loadmodule {mod} SAFEMODE".format(mod=REDISEARCH_SO)
    redis_server_subp = subprocess.Popen(shlex.split(redis_server_cmd))

    time.sleep(1)
    time.sleep(40) # wait until all data loaded
    perf_subp = fgraph.attach(pid=redis_server_subp.pid, freq=999)

    time.sleep(1)
    cmd = "/users/jhe/workdir/go/bin/RediSearchBenchmark "\
            "-engine redis -shards 1 -hosts \"localhost:6379\" "\
            "-duration 60 "\
            "-benchmark search -queries \"hello\" -c 32 -o /tmp/out.csv"
    print cmd
    subprocess.call(shlex.split(cmd))

    time.sleep(1)

    fgraph.detach(perf_subp)
    redis_server_subp.terminate()
    redis_server_subp.wait()

    fgraph.produce_graph("flame.svg")

    shcmd("cp ./perf_workdir/flame.svg /tmp/results/flame/")

if __name__ == "__main__":
    # main_debug()
    main_no_perf()
    # main_gdb()
    # main_perf()

