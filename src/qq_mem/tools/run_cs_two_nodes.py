import subprocess
import time
import shlex
from pyreuse.helpers import shcmd, cd

server_addr = "node.conan-wisc.fsperfatscale-pg0"
remote_addr = "node.conan-wisc-2.fsperfatscale-pg0"
n_server_threads = 32
n_client_threads = 32
search_engine = "vacuum:vacuum_dump:/mnt/ssd/vacuum_engine_dump_magic"

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
    print "-" * 20
    with cd("/users/jhe/flashsearch/src/qq_mem"):
        p = subprocess.Popen(shlex.split(
            "./build/qq_server -sync_type=ASYNC -n_threads={n_threads} -addr={server} -port=50051 -engine={engine}"
            .format(server = server_addr,
                    n_threads = n_server_threads,
                    engine = search_engine)))
        return p

def main():
    kill_client()

    compile_engine_bench()
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



