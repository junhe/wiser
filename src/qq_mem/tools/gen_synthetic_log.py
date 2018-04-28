import math
import random
import pickle

# DEBUG = True
DEBUG = False


def rand_item(l):
    total = len(l)
    i = random.randint(0, total - 1)
    return l[i]

def write_set_to_file(term_set, path):
    with open(path, "w") as f:
        for term in term_set:
            f.write(term + "\n")


class Buckets(object):
    def __init__(self):
        self.buckets = {}
        for i in range(10):
            self.buckets[i] = []

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

            self.add_to_bucket(freq_exp, term)


        print "Loading finished!"

        f.close()

    def random_pick(self, bucket_index):
        return rand_item(self.buckets[bucket_index])

    def add_to_bucket(self, exp, term):
        self.buckets[exp].append(term)

    def dump(self, path):
        ret = raw_input("are you sure to overwrite?")
        if ret == "y":
            pickle.dump( self.buckets, open( path, "wb" ) )

    def load(self, path):
        self.buckets = pickle.load( open( path, "rb" ) )

    def dump_bucket(self, i, path):
        random.shuffle(self.buckets[i])
        write_set_to_file(self.buckets[i], path)

    def create_set(self, exp, n):
        set_exp = set()

        loop_cnt = 0
        while len(set_exp) < n:
            loop_cnt += 1
            set_exp.add(self.random_pick(exp))
            if loop_cnt % 1000 == 0:
                print "loop cnt: ", loop_cnt, "set size:", len(set_exp)

        return set_exp


def main():
    buckets = Buckets()
    buckets.load_term_list("/mnt/ssd/popular_terms")

    for i in range(0, 7):
        print "dumping", i
        buckets.dump_bucket(i, "unique_terms_1e" + str(i))


if __name__ == "__main__":
    main()


