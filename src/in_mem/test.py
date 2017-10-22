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


class TestTokenizer(unittest.TestCase):
    def test(self):
        tokenizer = Tokenizer()
        self.assertEqual(tokenizer.tokenize("hello world!"), ['hello', 'world!'])


class TestIndexWriter(unittest.TestCase):
    def test(self):
        index = Index()
        doc_store = DocStore()
        tokenizer = Tokenizer()

        index_writer = IndexWriter(index, doc_store, tokenizer)
        index_writer.add_doc({
            "title": "This is my title",
            "text": "This is my body"})

        self.assertEqual(len(index.inverted_index), 5)
        self.assertEqual(len(doc_store.docs), 1)

        searcher = Searcher(index, doc_store)

        doc_ids = searcher.search(['This', 'is'], "AND")
        self.assertEqual(len(doc_ids), 1)

        doc_ids = searcher.search(['This', 'is', 'xxx'], "AND")
        self.assertEqual(len(doc_ids), 0)


if __name__ == '__main__':
    unittest.main()











