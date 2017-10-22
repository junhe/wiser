import unittest

from search_engine import *

class TestPostingList(unittest.TestCase):
    def test(self):
        pl = PostingList()
        pl.update_posting(8, {'frequency': 18})

        payload = pl.get_payload_dict(8)
        self.assertEquals(payload['frequency'], 18)
        self.assertEquals(pl.get_doc_id_set(), set([8]))


class TestDocStore(unittest.TestCase):
    def test(self):
        store = DocStore()
        doc0 = {'title': 'Hello Doc', 'text': 'My text'}
        doc_id = store.add_doc(doc0)

        doc1 = store.get_doc(doc_id)
        self.assertDictEqual(doc0, doc1)

class TestIndex(unittest.TestCase):
    def test(self):
        index = Index()
        index.add_doc(7, ['hello', 'world', 'good', 'bad'])
        postinglist = index.get_postinglist('hello')

        self.assertDictEqual(postinglist.get_payload_dict(7), {})
        self.assertEqual(index.get_doc_id_set('hello'), set([7]))
        self.assertEqual(index.get_doc_id_set('helloxxx'), set())

    def test_search(self):
        index = Index()
        index.add_doc(7, ['hello', 'world', 'good', 'bad'])
        index.add_doc(8, ['good', 'bad', 'ok', 'now'])

        self.assertListEqual(index.search(["ok"], 'AND'), [8])
        self.assertSetEqual(set(index.search(["good"], 'AND')), set([7, 8]))
        self.assertSetEqual(set(index.search(["hello", "world"], 'AND')), set([7]))
        self.assertListEqual(index.search(["iisjxjk"], 'AND'), [])


if __name__ == '__main__':
    unittest.main()
