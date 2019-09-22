from collections import Counter
import math

hist = Counter()

with open("/mnt/ssd/reddit-20m-docs.term-hist") as f:
    line_index = 0
    for line in f:
        term, cnt = line.split()
        exp = int(math.log(int(cnt), 10))
        hist[exp] += 1

        line_index += 1

        # if line_index == 200:
            # break

        if line_index % 1000000 == 0:
            print line_index


print hist
max_num = max(hist.keys())

for i in range(0, max_num + 1):
    print i, hist[i]




