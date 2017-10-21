import unittest

from search_engine import *

class TestPostingList(unittest.TestCase):
    def test(self):
        pl = PostingList()
        pl.update_posting(8, {'frequency': 18})

        payload = pl.get_payload_dict(8)
        self.assertEquals(payload['frequency'], 18)


class TestDocStore(unittest.TestCase):
    def test(self):
        store = DocStore()
        doc0 = {'title': 'Hello Doc', 'text': 'My text'}
        doc_id = store.add_doc(doc0)

        doc1 = store.get_doc(doc_id)
        self.assertDictEqual(doc0, doc1)


if __name__ == '__main__':
    unittest.main()
