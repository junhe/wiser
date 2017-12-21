import pprint
import redis
import os
import datetime
import time

from elasticsearch import Elasticsearch
from redisearch import Client, TextField, NumericField, Query

from expbase import Experiment
from utils.utils import LineDocPool, QueryPool, index_wikiabs_on_elasticsearch
from pyreuse import helpers


class RediSearchClient(object):
    def __init__(self, index_name):
        self.client = Client(index_name)
        self.index_name = index_name

    def build_index(self, line_doc_path, n_docs):
        line_pool = LineDocPool(line_doc_path)

        try:
            self.client.drop_index()
        except:
            pass

        self.client.create_index([TextField('title'), TextField('url'), TextField('body')])

        for i, d in enumerate(line_pool.doc_iterator()):
            self.client.add_document(i, nosave = True, title = d['doctitle'],
                    url = d['url'], body = d['body'])

            if i + 1 == n_docs:
                break

            if i % 1000 == 0:
                print "{}/{} building index".format(i, n_docs)

    def search(self, query):
        q = Query(query).paging(0, 5).verbatim()
        res = self.client.search(q)
        # print res.total # "1"
        return res


class RedisClient(object):
    def __init__(self, index_name):
        self.index_name = index_name
        self.client = redis.StrictRedis(host='localhost', port=6379, db=0)

    def search(self, query):
        ret = self.client.execute_command("FT.SEARCH", self.index_name, query,
                "LIMIT", 0, 5, "WITHSCORES", "VERBATIM")
        return ret


class ElasticSearchClient(object):
    def __init__(self, index_name):
        self.es_client = Elasticsearch()
        self.index_name = index_name

    def search(self, query_string):
        body = {
            "_source": False,
            "size": 0, # setting this to 0  will get 5x speedup
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

