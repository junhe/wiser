import sys
import requests, json

class EsIndex:
    def __init__(self):
        self.url = "http://localhost:9200"
        self.index_name = "my_index"
        self.headers = {'Content-Type': 'application/json'}

    def index_doc(self, doc_id, text):
        url = '{}/{}/doc/{}?pretty'.format(self.url, self.index_name, doc_id)
        payload = """
        { "text" : "%s" }
        """ % (text)

        r = requests.put(url, payload, headers=self.headers)
        print(r.text)

    def setup_index(self):
        url = '{}/{}?pretty'.format(self.url, self.index_name)
        payload = {
	    "settings" : {
		"index" : {
		    "number_of_shards" : 1,
		    "number_of_replicas" : 1
		}
	    }
	}
        r = requests.put(url, json=payload, headers=self.headers)
        print(r.text)

    def show_health(self):
        "curl -XGET 'localhost:9200/_cat/health?v&pretty'"
        url = '{}/_cat/health?v&pretty'.format(self.url)
        r = requests.get(url)
        print(r.text)

    def refresh(self):
        url = '{}/{}/_refresh?pretty'.format(self.url, self.index_name)

        print 'refreshing...'
        r = requests.post(url)
        print(r.text)

    def delete(self):
        url = '{}/{}?pretty'.format(self.url, self.index_name)

        r = requests.delete(url)
        print(r.text)

    def explain(self, terms, doc_id):
        url = '{}/{}/doc/{}/_explain?pretty'.format(self.url, self.index_name, doc_id)
        self._query(url, terms)

    def search(self, terms):
        url = '{}/{}/_search?pretty&search_type=dfs_query_then_fetch'.format(self.url, self.index_name)
        self._query(url, terms)

    def _query(self, url, terms):
        matches = []
        for term in terms:
            match = { "match": { "text": term } }
            matches.append(match)

        payload = {
                "query": {
                    "bool": {
                        "must": matches
                        }
                    }
                }

        print payload
        r = requests.get(url, json=payload, headers=self.headers)
        print(r.text)




if __name__=='__main__':
    es = EsIndex()
    es.delete()
    es.setup_index()
    es.show_health()
    es.index_doc(1, "hello world")
    es.index_doc(2, "hello wisconsin")
    es.index_doc(3, "hello world big world")
    es.refresh()

    # es.explain(['hello'], 1)
    print "------- search --------"
    # es.search(['wisconsin'])
    # es.explain(['wisconsin'], 1)
    # es.explain(['wisconsin'], 2)
    es.explain(['hello'], 3)
    # es.explain(['hello'], 2)
    # es.explain(['wisconsin'], 3)
    # es.search(['hello'])


