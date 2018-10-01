from pyreuse.helpers import *
import glob


def run_engine(log_path, factor):
    with cd("./build/"):
        cmd = "./engine_bench -exp_mode=locallog -query_path={path} " \
            "-bloom_factor={factor}".format(path=log_path, factor=factor)
        shcmd(cmd)


def main():
    shcmd("make engine_bench_build")
    # paths = glob.glob("/mnt/ssd/query_log/single-term/*") + \
        # glob.glob("/mnt/ssd/query_log/phrases/*")

    paths = ["/mnt/ssd/query_log/cleaned_querylog"]
    for path in paths:
        run_engine(path, 0)
        # run_engine(path, 5)
        # run_engine("/mnt/ssd/query_log/single-term/random_10")


if __name__ == "__main__":
    main()






