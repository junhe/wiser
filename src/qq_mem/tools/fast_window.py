from collections import Counter

class HashSet(object):
    def __init__(self):
        self.counter = Counter()

    def add(self, key):
        self.counter[key] += 1

    def remove(self, key):
        if self.counter[key] > 0:
            self.counter[key] -= 1

    def has(self, key):
        return self.counter[key] > 0


class Window(object):
    def __init__(self):
        self.list = []
        self.set = HashSet()

    def append(self, key):
        self.list.append(key)
        self.set.add(key)

    def pop(self):
        k = self.list[0]
        del self.list[0]
        self.set.remove(k)

    def has(self, key):
        return self.set.has(key)



