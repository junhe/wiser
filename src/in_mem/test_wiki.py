import unittest

from wiki import *

class TestLineDocPool(unittest.TestCase):
    def setUp(self):
        self.pool = LineDocPool("./testdata/linedoc-sample")

    def test_column_names(self):
        self.assertListEqual(self.pool.col_names, ["doctitle", "docdate", "body"])

    def test_line_to_dict(self):
        # sample_line = open("./testdata/linedoc-sample").readlines()[1]
        sample_line = "col1\tcol2\tcol3"
        doc_dicts = self.pool.line_to_dict(sample_line)
        self.assertDictEqual(doc_dicts, {'doctitle': 'col1', 'docdate': 'col2', 'body': 'col3'})

    def test_line_to_dict_real_data(self):
        sample_line = open("./testdata/linedoc-sample").readlines()[1]

        # check the method used in line_to_dict()
        items = sample_line.split("\t")
        self.assertEqual(len(items), 3)

        doc_dict = self.pool.line_to_dict(sample_line)
        self.assertSetEqual(set(doc_dict.keys()), set(["doctitle", "docdate", "body"]))
        self.assertEqual(doc_dict["doctitle"], "Anarchism")
        self.assertEqual(doc_dict["docdate"], "29-SEP-2017 17:40:38.000")
        self.assertEqual(len(doc_dict["body"]), 180734)

    def test_doc_iterator(self):
        docs = list(self.pool.doc_iterator())
        self.assertEqual(len(docs), 9)

        for doc in docs:
            self.assertSetEqual(set(doc.keys()), set(["doctitle", "docdate", "body"]))


if __name__ == '__main__':
    unittest.main()











