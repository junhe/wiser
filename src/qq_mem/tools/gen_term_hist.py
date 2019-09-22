from collections import Counter

line_doc_path = "/mnt/hdd/reddit/redditoverall_tokenized_preprocessed_pre_after"
term_cnt_path = "/mnt/ssd/reddit-2000-docs.term-hist"
n = 2000

term_cnt = Counter()
with open(line_doc_path) as f:
    line_index = 0
    parsed = 0
    for line in f:
        if line_index == 0:
            line_index += 1
            continue

        tokens = line.split("\t")[2]
        for token in tokens.split():
            term_cnt[token] += 1
        parsed += 1

        line_index += 1

        if line_index % 1000000 == 0:
            print "{:,}".format(line_index)

        if parsed == n:
            break


# print term_cnt

f = open(term_cnt_path, "w")
n_terms = 0
for term, cnt in term_cnt.items():
    f.write(term + " " + str(cnt) + "\n")
    n_terms += 1

    if n_terms % 100000 == 0:
        print "{:,}".format(n_terms), "/", "{:,}".format(len(term_cnt))

f.close()



