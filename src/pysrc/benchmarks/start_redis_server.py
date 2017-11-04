from .go_rsbench import SERVER_PATH, REDISEARCH_SO
import subprocess
import sys


def start_redis(port):
    cmd = "{server} --port {port} --loadmodule {mod}".format(
            server=SERVER_PATH, mod=REDISEARCH_SO, port=port)
    p = subprocess.Popen(cmd, shell = True)
    return p


def main():
    if len(sys.argv) != 2:
        print "Usage: please set the number of redis servers to start"
        exit(1)

    n = int(sys.argv[1])
    procs = []
    for i in range(n):
        procs.append(start_redis(6379 + i))

    for p in procs:
        p.wait()


if __name__ == "__main__":
    main()

