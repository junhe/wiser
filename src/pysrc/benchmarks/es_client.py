from elasticsearch import Elasticsearch
import sys

INDEX_NAME = "wik"

class ElasticSearchClient(object):
    def __init__(self, index_name):
        self.es_client = Elasticsearch()
        self.index_name = index_name

    def search(self, query_string):
        body = {
            "_source": True,
            "size": 5, # setting this to 0  will get 5x speedup
            "query": {
                "query_string" : {
                    # "fields" : ["body"],
                    "query" : query_string
                }
            }
        }

        response = self.es_client.search(
            index=self.index_name,
            body=body
        )

        return response

    def delete_index(self):
        ret = self.es_client.indices.delete(index=self.index_name)
        assert ret['acknowledged'] == True

    def clear_cache(self):
        ret = self.es_client.indices.clear_cache(index=self.index_name)

    def get(self):
        return self.es_client.indices.get(self.index_name)

    def get_number_of_shards(self):
        d = self.get()
        return d[self.index_name]['settings']['index']['number_of_shards']

    def build_index(self, line_doc_path, n_docs):
        line_pool = LineDocPool(line_doc_path)

        for i, d in enumerate(line_pool.doc_iterator()):
            del d['docdate']

            if i == n_docs:
                break

            res = self.es_client.index(index=self.index_name, doc_type='articles', id=i, body=d)

            if i % 100 == 0:
                print "{}/{}".format(i, n_docs)
        print


def worker(query_pool, query_count, engine):
    client = create_client(engine)

    for i in range(query_count):
        query = query_pool.next_query()
        ret = client.search(query)
        if i % 5000 == 0:
            print os.getpid(), "{}/{}".format(i, query_count)

def create_client(engine):
    if engine == "elastic":
        return ElasticSearchClient(INDEX_NAME)
    elif engine == "redis":
        # return RediSearchClient(INDEX_NAME)
        return RedisClient(INDEX_NAME)


def main():
    path = sys.argv[1]

    client = create_client("elastic")
    with open(path) as f:
        cnt = 0
        for line in f:
            # print line.strip()
            ret = client.search(line.strip())
            cnt += 1
            if cnt % 1000 == 0:
                print "finished", cnt

    print "ExperimentFinished!!!"


if __name__ == "__main__":
    main()

