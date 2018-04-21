import subprocess
import shlex
from pyreuse.helpers import shcmd, cd

server_addr = "node.conan-wisc.fsperfatscale-pg0"
remote_addr = "node.conan-wisc-2.fsperfatscale-pg0"

def remote_cmd_chwd(dir_path, cmd):
    return remote_cmd("cd {}; {}".format(dir_path, cmd))

def remote_cmd(cmd):
    print "starting command on remote node.... ", cmd
    p = subprocess.Popen(["ssh", "node.conan-wisc-2.fsperfatscale-pg0", cmd])
    return p


def sync_build_dir():
    build_path = "/users/jhe/flashsearch/src/qq_mem/build/engine_bench"
    shcmd("rsync -avzh --progress {path} {addr}:{path}".format(
        addr=remote_addr, path=build_path))


def start_client():
    return remote_cmd_chwd("/users/jhe/flashsearch/src/qq_mem/build/",
            # "pwd; hostname")
        "./engine_bench -exp_mode=grpclog -n_threads=16 -use_profiler=true "
        "-grpc_server={server}".format(server=server_addr))

def compile_engine_bench():
    shcmd("make engine_bench_build")

def start_server():
    with cd("/users/jhe/flashsearch/src/qq_mem"):
        p = subprocess.Popen(shlex.split("./build/qq_server -sync_type=ASYNC -n_threads=4"))
        return p


def main():
    # p = remote_cmd_chwd("/users/jhe/", "ls -l")
    # p.wait()
    # start_client()

    start_server()

    # compile_engine_bench()
    # sync_build_dir()
    # client_p = start_client()
    # client_p.wait()

if __name__ == "__main__":
    main()



