import subprocess
import time
import shlex
from pyreuse.helpers import shcmd, cd

server_addr = "node.conan-wisc.fsperfatscale-pg0:50051"
remote_addr = "node.conan-wisc-2.fsperfatscale-pg0"

def remote_cmd_chwd(dir_path, cmd):
    return remote_cmd("cd {}; {}".format(dir_path, cmd))

def remote_cmd(cmd):
    print "starting command on remote node.... ", cmd
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
            # "pwd; hostname")
        "./engine_bench -exp_mode=grpclog -n_threads=64 -use_profiler=true "
        "-grpc_server={server}".format(server=server_addr))

def start_server():
    print "-" * 20
    print "starting server ..."
    print "-" * 20
    with cd("/users/jhe/flashsearch/src/qq_mem"):
        p = subprocess.Popen(shlex.split("./build/qq_server -sync_type=ASYNC -n_threads=64"))
        return p

def main():
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



