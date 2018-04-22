import subprocess, os
import time
import shlex
from pyreuse.helpers import *
from pyreuse.sysutils.cgroup import *
from pyreuse.macros import *

server_addr = "node.conan-wisc.fsperfatscale-pg0"
remote_addr = "node.conan-wisc-2.fsperfatscale-pg0"
n_server_threads = 32
n_client_threads = 32
search_engine = "vacuum:vacuum_dump:/mnt/ssd/vacuum_engine_dump_magic"
# search_engine = "qq_mem_compressed"
profile_qq_server = "true"
server_mem_size = 300 * MB


gprof_env = os.environ.copy()
gprof_env["CPUPROFILE_FREQUENCY"] = '1000'


class Cgroup(object):
    def __init__(self, name, subs):
        self.name = name
        self.subs = subs

        shcmd('sudo cgcreate -g {subs}:{name}'.format(subs=subs, name=name))

    def set_item(self, sub, item, value):
        """
        Example: sub='memory', item='memory.limit_in_bytes'
        echo 3221225472 > memory/confine/memory.limit_in_bytes
        """
        path = self._path(sub, item)
        shcmd("echo {value} |sudo tee {path}".format(value = value, path = path))

        # self._write(sub, item, value)

        ret_value = self._read(sub, item)

        if ret_value != str(value):
            print 'Warning:', ret_value, '!=', value

    def get_item(self, sub, item):
        return self._read(sub, item)

    def execute(self, cmd):
        cg_cmd = ['sudo', 'cgexec',
                '-g', '{subs}:{name}'.format(subs=self.subs, name=self.name),
                '--sticky']
        cg_cmd += cmd
        print cg_cmd
        p = subprocess.Popen(cg_cmd)

        return p

    def _write(self, sub, item, value):
        path = self._path(sub, item)
        with open(path, 'w') as f:
            f.write(str(value))

    def _read(self, sub, item):
        path = self._path(sub, item)
        with open(path, 'r') as f:
            value = f.read()

        return value.strip()

    def _path(self, sub, item):
        path = os.path.join('/sys/fs/cgroup', sub, self.name, item)
        return path



def remote_cmd_chwd(dir_path, cmd):
    return remote_cmd("cd {}; {}".format(dir_path, cmd))

def remote_cmd(cmd):
    print "Starting command on remote node.... ", cmd
    p = subprocess.Popen(["ssh", remote_addr, cmd])
    return p

def sync_build_dir():
    build_path = "/users/jhe/flashsearch/src/qq_mem/build/engine_bench"
    shcmd("rsync -avzh --progress {path} {addr}:{path}".format(
        addr=remote_addr, path=build_path))

def compile_engine_bench():
    shcmd("make engine_bench_build")

def start_client():
    print "-" * 20
    print "starting client ..."
    print "-" * 20
    return remote_cmd_chwd("/users/jhe/flashsearch/src/qq_mem/build/",
        "./engine_bench -exp_mode=grpclog -n_threads={n_threads} -use_profiler=true "
        "-grpc_server={server}"
        .format(server = server_addr, n_threads = n_client_threads))

def kill_client():
    p = remote_cmd("pkill engine_bench")
    p.wait()

def start_server():
    print "-" * 20
    print "starting server ..."
    print "server mem size:", server_mem_size
    print "-" * 20

    cg = Cgroup(name='charlie', subs='memory')
    cg.set_item('memory', 'memory.limit_in_bytes', server_mem_size)

    with cd("/users/jhe/flashsearch/src/qq_mem"):
        cmd = "./build/qq_server "\
              "-sync_type=ASYNC -n_threads={n_threads} "\
              "-addr={server} -port=50051 -engine={engine} -use_profiler={profile}"\
              .format(server = server_addr,
                    n_threads = n_server_threads,
                    engine = search_engine,
                    profile = profile_qq_server)
        print "-" * 20
        print "server cmd:", cmd
        print "-" * 20
        # p = subprocess.Popen(shlex.split(cmd), env = gprof_env)
        p = cg.execute(shlex.split(cmd))
        return p


def main():
    kill_client()

    # we already are doing it in Makefile, just make sure you run this script by `make two_nodes`
    # compile_engine_bench()

    sync_build_dir()

    server_p = start_server()

    print "Wating for some time util the server starts...."
    time.sleep(30)

    client_p = start_client()

    print "wating for some other time...."
    time.sleep(5)

    client_p.wait()
    server_p.wait()

if __name__ == "__main__":
    main()

