from .rs_bench_go import start_redis
import sys

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

