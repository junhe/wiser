import unittest

from fast_window import *

class TestFastWindow(unittest.TestCase):
    def test_HashSet(self):
        my_set = HashSet()
        my_set.add(1)
        self.assertTrue(my_set.has(1))

        my_set.remove(1)
        self.assertFalse(my_set.has(1))

        my_set.remove(1)
        self.assertFalse(my_set.has(1))

    def test_Window(self):
        win = Window()
        win.append(1)
        self.assertTrue(win.has(1))

        win.append(1)
        self.assertTrue(win.has(1))

        win.pop() # will pop 1
        self.assertTrue(win.has(1))

        win.pop() # will pop 1
        self.assertFalse(win.has(1))

    def test_Window2(self):
        win = Window()

        win.append(3)
        win.append(2)
        win.append(2)
        win.append(1)

        self.assertListEqual(win.list, [3, 2, 2, 1])

        self.assertTrue(win.has(3))
        self.assertTrue(win.has(2))
        self.assertTrue(win.has(1))

        win.pop()
        self.assertListEqual(win.list, [2, 2, 1])

        self.assertFalse(win.has(3))
        self.assertTrue(win.has(2))
        self.assertTrue(win.has(1))

        win.pop()
        self.assertListEqual(win.list, [2, 1])

        self.assertFalse(win.has(3))
        self.assertTrue(win.has(2))
        self.assertTrue(win.has(1))

        win.pop()
        self.assertListEqual(win.list, [1])

        self.assertFalse(win.has(3))
        self.assertFalse(win.has(2))
        self.assertTrue(win.has(1))

        win.pop()
        self.assertListEqual(win.list, [])

        self.assertFalse(win.has(3))
        self.assertFalse(win.has(2))
        self.assertFalse(win.has(1))


