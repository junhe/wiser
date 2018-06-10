from pyreuse.sysutils.filefragparser import *
import pprint


class Translator(object):
    def __init__(self, file_path, in_file_mapping_path):
        self.type_table = parse_pl_mapping(open(in_file_mapping_path).read())
        self.logical_physical_table = get_logical_phyical_mapping(file_path)
        print self.type_table
        print self.logical_physical_table

    def translate(self, blk_offset):
        file_off = self.blk_off_to_file_off(blk_offset)
        return self.file_offset_to_type(file_off)

    def blk_off_to_file_off(self, off):
        for row in self.logical_physical_table:
            if off >= row['physical_start'] and \
                    off < row['physical_start'] + row['length']:
                off_in_seg = off - row['physical_start']
                return row['logical_start'] + off_in_seg
        return None

    def file_offset_to_type(self, offset):
        for row in self.type_table:
            if offset >= row['start'] and offset < row['start'] + row['size']:
                return row['type']
        return None



def get_logical_phyical_mapping(file_path):
    table = filefrag(file_path)
    for i in range(len(table)):
        table[i]['logical_start'] = table[i]['logical_start'] * 4096
        table[i]['physical_start'] = table[i]['physical_start'] * 4096
        table[i]['length'] = table[i]['length'] * 4096

    return table


def parse_pl_mapping(txt):
    header = ["term", "start", "size", "type"]

    table = []
    for line in txt.split("\n"):
        if len(line.strip()) == 0:
            continue

        items = line.split(',')
        items[1] = int(items[1])
        items[2] = int(items[2])
        d = dict(zip(header, items))
        table.append(d)

    return table


def main():
    translator = Translator("/mnt/ssd/vacuum-06-08-with-bloom-empty-set/my.vacuum",
            "./pl-mapping.txt")
    with open("/tmp/results/exp-2018-06-09-16-51-42.678072/1528584705.99/trace-table.txt") as f:
        for line in f:
            items = line.split()
            offset = items[2]
            print translator.translate(int(offset))

if __name__ == "__main__":
    main()

