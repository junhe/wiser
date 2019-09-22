from collections import Counter

term_cnt = Counter()
with open("/mnt/ssd/reddit-20m-docs.term-hist") as f:
    for line in f:
        term, cnt = line.split()
        term_cnt[term] = int(cnt)
        if term == "i":
            print "we got i", cnt

f = open("/mnt/ssd/reddit-most-popular-all", "w")
commons = term_cnt.most_common()

for term, cnt in commons:
    f.write(term + " " + str(cnt) + "\n")

f.close()




