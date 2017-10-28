from utils.utils import LineDocPool
from redisearch import Client, TextField, NumericField, Query



class RedisIndex(object):
    def __init__(self, line_doc_path, n_docs):
        # self.line_pool = LineDocPool("/mnt/ssd/downloads/linedoc_tokenized")
        self.line_pool = LineDocPool(line_doc_path)
        self.n_docs = n_docs

    def create_index(self):
        client = Client('wiki')
        client.drop_index()
        client.create_index([TextField('title', weight=5.0), TextField('body')])

        for i, d in enumerate(self.line_pool.doc_iterator()):
            client.add_document(i, title = d['doctitle'], body = d['body'])

            if i + 1 == self.n_docs:
                break

            if i % 100 == 0:
                print i
            break

        self.client = client

    def search(self, query):
        res = self.client.search(query)
        print res.total # "1"
        print res.docs[0].title


def main():
    index = RedisIndex("/mnt/ssd/downloads/linedoc_tokenized", 100)
    index.create_index()
    index.search("freethought")


if __name__ == "__main__":
    main()


