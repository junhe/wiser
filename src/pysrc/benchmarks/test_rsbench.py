import unittest
from .rs_bench import *


@unittest.skip("Requires Redis server running")
class TestRedisIndex(unittest.TestCase):
    def test(self):
        index = RedisIndex("wiki")
        print index.search("hello").total

if __name__ == "__main__":
    unittest.main()

