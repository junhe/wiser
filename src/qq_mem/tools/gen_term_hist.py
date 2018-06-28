from collections import Counter

term_cnt = Counter()
with open("/mnt/hdd/reddit/redditoverall_tokenized_preprocessed_pre_after") as f:
    line_index = 0
    for line in f:
        if line_index == 0:
            line_index += 1
            continue

        tokens = line.split("\t")[2]
        for token in tokens.split():
            term_cnt[token] += 1

        line_index += 1

        if line_index % 1000000 == 0:
            print "{:,}".format(line_index)

        if line_index == 20000001:
            break


# print term_cnt

f = open("/mnt/ssd/reddit-20m-docs.term-hist", "w")
n_terms = 0
for term, cnt in term_cnt.items():
    f.write(term + " " + str(cnt) + "\n")
    n_terms += 1

    if n_terms % 100000 == 0:
        print "{:,}".format(n_terms), "/", "{:,}".format(len(term_cnt))

f.close()



