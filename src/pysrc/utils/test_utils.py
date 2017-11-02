import unittest
from .utils import *


class TestWikiAbstract(unittest.TestCase):
    def test(self):
        wiki = WikiAbstract("utils/testdata/enwiki-abstract-sample.xml")
        entries = list(wiki.entries())
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]['title'], "Wikipedia: Anarchism")


if __name__ == '__main__':
    unittest.main()

