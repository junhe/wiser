import os
from pyreuse.helpers import *


def parse(log_path):
    f = open(log_path)
    lines = f.readlines();
    f.close()

    print len(lines)

    i = 0
    n = len(lines)

    state = 1
    cur_logname = None
    cur_bloomfactor = None

    table = []
    while i < n:
        if "Bloom factor:" in lines[i]:
            cur_bloomfactor = lines[i].split()[-1]
            i += 1
        elif "Loading query log from" in lines[i]:
            cur_logname = os.path.basename(lines[i].split()[-1])
            i += 1
        elif "bloom_after" in lines[i] and "bloom_before" in lines[i]:
            keys = lines[i].split()
            vals = lines[i+1].split()
            d = dict([(k, int(v)/(1024*1024)) for k, v in zip(keys, vals)])
            d["filename"] = cur_logname
            d["bloomfactor"] = cur_bloomfactor
            table.append(d)
            i += 2
        else:
            i += 1

    print table_to_str(table)


def main():
    # parse("log.old")
    parse("logtoday")


if __name__ == "__main__":
    main()











