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

    def size(self):
        return len(self.list)


def analyze_window(sequence, win_size):
    in_win = 0
    total = 0
    window = Window()

    for item in sequence:
        if window.size() < win_size:
            window.append(item)
            continue

        if window.has(item):
            in_win += 1
        total += 1

        window.pop()
        window.append(item)

    d = {
            'window size': win_size,
            'in_win': in_win,
            'total': total,
            'seq_size': len(sequence),
            'ratio': in_win * 1.0 / total
        }
    return d

