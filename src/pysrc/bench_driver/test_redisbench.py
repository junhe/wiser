import unittest
from .redisearch_bench import *


class TestRedisIndex(unittest.TestCase):
    def test(self):
        index = RedisIndex("wiki")
        print index.search("hello").total

if __name__ == "__main__":
    unittest.main()

