from pprint import pprint

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

def analyze(block_offs, win_size):
    window = []
    in_window_cnt = 0
    total_cnt = 0
    for off in block_offs:
        if len(window) < win_size:
            window.append(off)
            continue

        assert len(window) == win_size

        if off in window:
            in_window_cnt += 1
        total_cnt += 1

        del window[0]
        window.append(off)

    print "----- window size:", win_size, "------------"
    print 'in_window_cnt:', in_window_cnt
    print 'total_cnt:', total_cnt
    print 'ratio:', in_window_cnt / float(total_cnt)


def main():
    table = read_table("/tmp/results/exp-2018-05-17-22-06-45.276065/1526616408.02/trace-table.txt")
    table = give_names(table)
    block_offs = split_chunks(table)
    analyze(block_offs, 1000)
    analyze(block_offs, 10000)

if __name__ == "__main__":
    main()

