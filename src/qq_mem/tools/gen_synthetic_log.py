import math
import copy
import random
import pickle
from pyreuse.helpers import *


"""
exp 0 num of terms: 4996891
exp 1 num of terms: 520675
exp 2 num of terms: 94721
exp 3 num of terms: 22139
exp 4 num of terms: 5717
exp 5 num of terms: 1434
exp 6 num of terms: 38
"""

# DEBUG = True
DEBUG = False

log_to_group = {
        0: "low",
        1: "low",
        2: "low",
        3: "low",
        4: "high",
        5: "high",
        6: "high"}


group_to_log = {}
for log, group in log_to_group.items():
    if group_to_log.has_key(group) is False:
        group_to_log[group] = [log]
    else:
        group_to_log[group].append(log)

def rand_item(l):
    total = len(l)
    i = random.randint(0, total - 1)
    return l[i]

def write_to_file(term_set, path):
    print "writing to", path
    with open(path, "w") as f:
        for term in term_set:
            f.write(term + "\n")

def rand_items_from_set(item_set, n):
    """
    pick n random items from item_set
    """
    set_size = len(item_set)
    items = []
    for cnt in range(n):
        i = random.randint(0, set_size - 1)
        items.append(item_set[i])
        if cnt % 10000 == 0:
            print "cnt:", cnt

    return items

class Buckets(object):
    def __init__(self):
        self.buckets = {}
        for i in range(10):
            self.buckets[i] = []
        self.groups = {'low': [], 'high': []}

    def load_term_list(self, path):
        print "loading....."
        f = open(path, "r")

        cnt = 0
        for line in f:
            cnt += 1

            if DEBUG is True:
                if cnt % 10000 != 0:
                    continue

            items = line.split()
            term = items[0]
            doc_freq = int(items[1])
            freq_exp = int(math.log(doc_freq, 10))

            # self.add_to_bucket(freq_exp, term)
            self.add_to_group(freq_exp, term)

        print "Loading finished!"

        f.close()

        self.show_stats()

    def show_stats(self):
        for k, v in self.buckets.items():
            print "exp", k, "num of terms:", len(v)

    def random_pick(self, bucket_index):
        return rand_item(self.buckets[bucket_index])

    def add_to_bucket(self, exp, term):
        self.buckets[exp].append(term)

    def add_to_group(self, exp, term):
        self.groups[log_to_group[exp]].append(term)

    def dump(self, path):
        ret = raw_input("are you sure to overwrite?")
        if ret == "y":
            pickle.dump( self.buckets, open( path, "wb" ) )

    def load(self, path):
        self.buckets = pickle.load( open( path, "rb" ) )

    def dump_bucket(self, i, path):
        random.shuffle(self.buckets[i])
        write_to_file(self.buckets[i], path)

    def create_set(self, bucket_index, n):
        """
        randomly get n terms from buckets[index]
        """
        terms = copy.copy(self.buckets[bucket_index])

        if len(terms) < n:
            raise RuntimeError("not enough terms in this bucket")

        random.shuffle(terms)
        return terms[0:n]

    def create_set_from_group(self, group_name, n):
        """
        randomly get n terms from a group
        """
        terms = copy.copy(self.groups[group_name])

        if n == "all":
            n = len(terms)

        if len(terms) < n:
            raise RuntimeError("not enough terms in this bucket")

        random.shuffle(terms)
        return terms[0:n]


def dump_buckets():
    buckets = Buckets()
    buckets.load_term_list("/mnt/ssd/popular_terms")

    for i in range(0, 7):
        print "dumping", i
        buckets.dump_bucket(i, "unique_terms_1e" + str(i))

def get_queries_from_working_set(buckets, bucket_index, n_uniq_terms, n_queries):
    # pick n_terms (working set), unique ones
    unique_terms = buckets.create_set(bucket_index, n_uniq_terms)

    print "*" * 30
    print "Number of unique terms:", len(unique_terms)
    print "Num of queries to generate:", n_queries
    print "*" * 30

    return rand_items_from_set(unique_terms, n_queries)


def single_term_queries(buckets):
    working_set_sizes = {
                    'low':   "all",
                    'high':  "all",
            }
    query_cnt  = {
                    'low':  1000000,
                    'high':  100000,
            }


    shcmd("rm -f /mnt/ssd/query_workload/single_term/*")

    for group_name in working_set_sizes.keys():
        print group_name
        working_set = buckets.create_set_from_group(
                group_name, working_set_sizes[group_name])
        print "working set size:", len(working_set)

        queries = rand_items_from_set(working_set, query_cnt[group_name])
        # write_to_file(queries, "/mnt/ssd/query_workload/single_term/type_single.docfreq_" + group_name)

def two_term_queries(buckets):
    n_queries = 100000

    queries = set()
    while len(queries) < n_queries:
        t1_group = rand_item(["low", "high"])
        t2_group = rand_item(["low", "high"])

        t1 = rand_item(buckets.groups[t1_group])
        t2 = rand_item(buckets.groups[t2_group])
        while t2 == t1:
            t2 = rand_item(buckets.groups[t2_group])
        query = sorted([t1, t2])
        queries.add(" ".join(query))

    print "number of queries:", len(queries)
    folder = "/mnt/ssd/query_workload/two_term/"
    shcmd("rm -f {}/*".format(folder))

    prepare_dir(folder)
    write_to_file(queries, os.path.join(folder, "type_twoterm"))

    print "Sanity check..."
    shcmd("head -n 10 " + os.path.join(folder, "type_twoterm"))

def find_all_unique_phrases():
    """
    579,151 phrases found
    """
    f = open("/mnt/ssd/english-phrases", "r")

    phrases = []
    term_set = set()
    for line in f:
        terms = line.split()
        if len(terms) != 2:
            continue
        if terms[0] in term_set:
            continue
        if terms[1] in term_set:
            continue
        if terms[0] == terms[1]:
            continue

        phrases.append(line.strip())
        term_set.add(terms[0])
        term_set.add(terms[1])
    f.close()

    write_to_file(phrases, "/mnt/ssd/query_workload/no-repeated-terms-queries.corrected")

    def test():
        terms = []
        with open("/mnt/ssd/query_workload/no-repeated-terms-queries.corrected") as f:
            for line in f:
                terms.extend(line.split())
        if not len(terms) == len(set(terms)):
            print len(terms), len(set(terms))
            raise RuntimeError("not unique!")
        print "Test passed"
    test()


def gen_phrase_queries():
    n_queries = 10000
    phrase_set = []
    with open("/mnt/ssd/query_workload/no-repeated-terms-queries") as f:
        for line in f:
            phrase_set.append(line.strip())

    phrases = rand_items_from_set(phrase_set, n_queries)
    phrases = ["\"" + phrase + "\"" for phrase in phrases]
    folder = "/mnt/ssd/query_workload/two_term_phrases/"
    prepare_dir(folder)
    write_to_file(phrases, os.path.join(folder, "type_phrase"))

def main():
    # buckets = Buckets()
    # buckets.load_term_list("/mnt/ssd/popular_terms")

    ## Single term
    # single_term_queries(buckets)

    ## Two term
    # two_term_queries(buckets)

    ## find phrases without repeated terms
    # find_all_unique_phrases()

    ## generatel phrases
    # gen_phrase_queries()

    find_all_unique_phrases()



if __name__ == "__main__":
    main()
    # pass


