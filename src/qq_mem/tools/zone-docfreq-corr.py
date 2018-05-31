

def get_doc_freq_dict(path):
    d = {}
    with open(path) as f:
        for line in f:
            items = line.split()
            d[items[0]] = items[1]
    return d


def build():
    doc_freq = get_doc_freq_dict("/mnt/ssd/popular_terms")
    out = open("term_docfreq_zonesize", "w")
    with open("/mnt/ssd/query_workload/sorted_by_prefetch_zone_sizes.txt") as f:
        for line in f:
            items = line.split()
            term = items[0]
            zone_size = items[1]
            freq = doc_freq[term]
            items.append(str(freq))
            out.write(" ".join(items) + "\n")

    out.close()


build()

