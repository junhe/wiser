from pprint import pprint
from pyreuse.general.sliding_window import *
from pyreuse.macros import *


def read_table(path):
    table = []
    with open(path) as f:
        for line in f:
            table.append(line.split())

    return sorted(table, key = lambda x: x[4])


def give_names(table):
    colnames = ['pid', 'operation', 'offset', 'size',
            'timestamp', 'pre_wait_time']

    table2 = []
    for row in table:
        d = dict(zip(colnames, row))
        table2.append(d)

    return table2

def split_request(offset, size):
    n_blocks = size / 4096
    ret = []
    for i in range(n_blocks):
        ret.append(offset + i * 4096)

    return ret

def split_chunks(table):
    offsets = []
    for row in table:
        block_offs = split_request(int(row['offset']), int(row['size']))
        offsets += block_offs

    return offsets

def analyze_workload(path, win_sizes):
    table = read_table(path)
    table = give_names(table)
    block_offs = split_chunks(table)

    print 'all:', len(block_offs)
    print 'unique:', len(set(block_offs))
    print 'ratio:', len(set(block_offs)) * 1.0 / len(block_offs)

    table = []
    for size in win_sizes:
        d = analyze_window(block_offs, size)
        d = make_it_human(d)
        table.append(d)
        print table

    return table


def to_gb(n):
    v = 1.0 * n * 4 * KB / GB
    return round(v, 2)

def to_mb(n):
    v = 1.0 * n * 4 * KB / MB
    return round(v, 2)

def make_it_human(d):
    """
    [{'in_win': 0,
    'total': 704880,
    'window size': 10,
    'ratio': 0.0,
    'seq_size': 704890}]
    """
    d['in_win_mb'] = to_mb(d['in_win'])
    d['in_win_gb'] = to_gb(d['in_win'])
    d['win_size_mb'] = to_mb(d['window size'])
    d['win_size_gb'] = to_gb(d['window size'])
    d['total_mb'] = to_mb(d['total'])
    d['total_gb'] = to_gb(d['total'])

    return d

def main():
    pprint(analyze_workload(
        "/tmp/results/exp-2018-05-22-12-17-37.421507/1527013529.74/trace-table.txt",
        [10, 100, 1000]))

if __name__ == "__main__":
    main()

