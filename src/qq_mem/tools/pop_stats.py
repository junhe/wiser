from collections import Counter
import math

table = {}
with open("/mnt/ssd/wiki/popular_terms-wiki-2018-May") as f:
    for line in f:
        term, count = line.split()
        table[term] = count


def lookup(table, term):
    try:
        return int(table[term])
    except KeyError:
        return 0.001

hist = Counter()
with open("/mnt/ssd/query_workload/two_term_phrases/type_phrase") as f:
    for line in f:
        line = line.strip().strip("\"")
        t1, t2 = line.split()
        # print t1, lookup(table, t1)
        # print t2, lookup(table, t2)
        exp = math.log(lookup(table, t1), 10)
        hist[int(exp)] += 1
        exp = math.log(lookup(table, t2), 10)
        hist[int(exp)] += 1


for i in range(10):
    print i, hist[i]
