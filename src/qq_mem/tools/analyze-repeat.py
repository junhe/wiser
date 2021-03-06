from pprint import pprint
from collections import Counter
from pyreuse.general.sliding_window import *


def analyze_workload(path, win_sizes):
    terms = []
    with open(path) as f:
        for line in f:
            terms.extend(line.strip("\"").split())

    print 'all:', len(terms)
    print 'unique:', len(set(terms))
    print 'ratio:', len(set(terms)) * 1.0 / len(terms)

    table = []
    for size in win_sizes:
        table.append(analyze_window(terms, size))
        print table

    return table


def test():
    with open("/tmp/tmp-workload", "w") as f:
        f.write("hello\n")
        f.write("world\n")
        f.write("hello\n")
        f.write("good\n")

    table = analyze_workload("/tmp/tmp-workload", [2])
    print table
    assert table[0]['window size'] == 2
    assert table[0]['in_win'] == 1
    assert table[0]['total'] == 2


def main():
    test()
    pprint( analyze_workload(
            # "/mnt/ssd/query_workload/single_term/type_single.docfreq_low",
            # "/mnt/ssd/query_workload/single_term/type_single.docfreq_high",
            # "/mnt/ssd/query_workload/two_term/type_twoterm",
            # "/mnt/ssd/query_workload/two_term_phrases/type_phrase.version_v2",
            "/mnt/ssd/query_workload/term.zone-size.256KBplus",
            [10, 100, 500]) )

if __name__ == "__main__":
    main()


