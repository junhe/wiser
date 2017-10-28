import unittest
from .esbench import *


class TestWikiClient(unittest.TestCase):
    def test_indexing(self):
        client = WikiClient()
        client.build_index("/mnt/ssd/downloads/linedoc_tokenized", 10)
        resp = client.search("anarchist")
        self.assertEqual(resp['hits']['total'], 1)



if __name__ == '__main__':
    unittest.main()



