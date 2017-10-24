import re

class QueryPool(object):
    def __init__(self, query_path):
        self.fd = open(query_path, 'r')

    def query_iter(self):
        for line in self.fd:
            query = line.strip()

            query = query.replace("&language=en", " ")
            query = re.sub("[^\w]", " ",  query)
            # query = query.replace("AND", " ")
            # format: "hello world"
            yield query.strip()



def main():
    pool = QueryPool("/mnt/ssd/downloads/wiki_QueryLog")
    out = open("/mnt/ssd/downloads/wiki_QueryLog.clean", "w")

    for query in pool.query_iter():
        out.write(query + "\n")

    out.close()


if __name__ == '__main__':
    main()


