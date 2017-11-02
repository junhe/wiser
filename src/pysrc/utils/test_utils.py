import unittest
from .utils import *


class TestWikiAbstract(unittest.TestCase):
    def test(self):
        wiki = WikiAbstract("utils/testdata/enwiki-abstract-sample.xml")
        entries = list(wiki.entries())
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]['title'], "Wikipedia: Anarchism")

    @unittest.skip("Too large a file")
    def test_large_file(self):
        wiki = WikiAbstract("/mnt/ssd/downloads/enwiki-20171020-abstract.xml")
        for entry in wiki.entries():
            print entry


class TestWikiAbstract2(unittest.TestCase):
    def test(self):
        wiki = WikiAbstract2("utils/testdata/enwiki-abstract-sample.xml")
        entries = list(wiki.entries())
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]['title'], "Wikipedia: Anarchism")



if __name__ == '__main__':
    unittest.main()

