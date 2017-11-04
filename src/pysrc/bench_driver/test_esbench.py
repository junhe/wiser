import unittest
import time

from .esbench import *


class TestWikiClient(unittest.TestCase):
    def test_indexing(self):
        client = WikiClient()
        client.delete_index()
        client.build_index("/mnt/ssd/downloads/linedoc_tokenized", 10)

        time.sleep(1) # index is not immediately available in ES

        resp = client.search("anarchist")
        self.assertEqual(resp['hits']['total'], 1)



if __name__ == '__main__':
    unittest.main()



