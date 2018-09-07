from pyreuse.helpers import *
import os
import copy

MILLION = 1000000

def build_creator():
    shcmd("make -j32 create_qq_mem_dump")

def build_convertor():
    shcmd("make -j32 convert_qq_to_vacuum")

def create(dump_type, bloom_entries, bloom_ratio, n_lines, line_doc_path, dump_dir_path):
    cmd = "./create_qq_mem_dump " \
            "-dump_type={dump_type} " \
            "-bloom_entries={bloom_entries} -bloom_ratio={bloom_ratio} " \
            "-n_lines={n_lines} " \
            "-line_doc_path={line_doc_path} " \
            "-dump_dir_path={dump_dir_path} "
    shcmd(cmd.format(
        dump_type = dump_type,
        bloom_entries = bloom_entries,
        bloom_ratio = bloom_ratio,
        n_lines = n_lines,
        line_doc_path = line_doc_path,
        dump_dir_path = dump_dir_path
        ))

def convert(use_bloom_filters, align_doc_store, qqdump_dir_path, vacuum_dir_path):
    cmd = "./convert_qq_to_vacuum "\
            "-use_bloom_filters={use_bloom_filters} "\
            "-align_doc_store={align_doc_store} "\
            "-qqdump_dir_path={qqdump_dir_path} "\
            "-vacuum_dir_path={vacuum_dir_path}"
    shcmd(cmd.format(use_bloom_filters = use_bloom_filters,
                     align_doc_store = align_doc_store,
                     qqdump_dir_path = qqdump_dir_path,
                     vacuum_dir_path = vacuum_dir_path))

class VacuumWiki(object):
    def __init__(self):
        self.conf = {
            "bloom_entries": 5,
            "bloom_ratio": 0.0009,
            "n_lines": 20000000,
            "line_doc_path": "/mnt/ssd/wiki_full.linedoc_tokenized_preprocessed_pre_after",
            "qqdump_dir_path": "/mnt/ssd/qq-wiki-06-24.bloom.5-0.0009",
            }

    def create_qq(self):
        create(dump_type = "inverted",
               bloom_entries = self.conf["bloom_entries"],
               bloom_ratio = self.conf["bloom_ratio"],
               n_lines = self.conf["n_lines"],
               line_doc_path = self.conf["line_doc_path"],
               dump_dir_path = self.conf["qqdump_dir_path"])

        create(dump_type = "bloom_begin",
               bloom_entries = self.conf["bloom_entries"],
               bloom_ratio = self.conf["bloom_ratio"],
               n_lines = self.conf["n_lines"],
               line_doc_path = self.conf["line_doc_path"],
               dump_dir_path = self.conf["qqdump_dir_path"])

        create(dump_type = "bloom_end",
               bloom_entries = self.conf["bloom_entries"],
               bloom_ratio = self.conf["bloom_ratio"],
               n_lines = self.conf["n_lines"],
               line_doc_path = self.conf["line_doc_path"],
               dump_dir_path = self.conf["qqdump_dir_path"])

    def convert_to_vacuum(self):
        # convert(use_bloom_filters = False,
                # align_doc_store = False,
                # qqdump_dir_path = self.conf["qqdump_dir_path"],
                # vacuum_dir_path = "/mnt/ssd/vacuum-wiki-06-24.baseline")

        # convert(use_bloom_filters = False,
                # align_doc_store = True,
                # qqdump_dir_path = self.conf["qqdump_dir_path"],
                # vacuum_dir_path = "/mnt/ssd/vacuum-wiki-06-24.plus.align")

        convert(use_bloom_filters = True,
                align_doc_store = True,
                qqdump_dir_path = self.conf["qqdump_dir_path"],
                vacuum_dir_path = "/mnt/ssd/vacuum-wiki-06-25.plus.align.bloom")


class VacuumReddit(object):
    def __init__(self):
        self.conf = {
            "bloom_entries": 5,
            "bloom_ratio": 0.0009,
            "n_lines": 20 * MILLION,
            # "n_lines": 2 * 1000,
            "line_doc_path": "/mnt/hdd/reddit/redditoverall_tokenized_preprocessed_pre_after",
            "qqdump_dir_path": "/mnt/ssd/qq-reddit-07-02-bloom-5-0.0009-20m/",
            }

    def create_qq(self):
        create(dump_type = "inverted",
               bloom_entries = self.conf["bloom_entries"],
               bloom_ratio = self.conf["bloom_ratio"],
               n_lines = self.conf["n_lines"],
               line_doc_path = self.conf["line_doc_path"],
               dump_dir_path = self.conf["qqdump_dir_path"])

        create(dump_type = "bloom_begin",
               bloom_entries = self.conf["bloom_entries"],
               bloom_ratio = self.conf["bloom_ratio"],
               n_lines = self.conf["n_lines"],
               line_doc_path = self.conf["line_doc_path"],
               dump_dir_path = self.conf["qqdump_dir_path"])

        create(dump_type = "bloom_end",
               bloom_entries = self.conf["bloom_entries"],
               bloom_ratio = self.conf["bloom_ratio"],
               n_lines = self.conf["n_lines"],
               line_doc_path = self.conf["line_doc_path"],
               dump_dir_path = self.conf["qqdump_dir_path"])

    def convert_to_vacuum(self):
        # convert(use_bloom_filters = False,
                # align_doc_store = False,
                # qqdump_dir_path = self.conf["qqdump_dir_path"],
                # vacuum_dir_path = "/mnt/ssd/vacuum-wiki-06-24.baseline")

        # convert(use_bloom_filters = False,
                # align_doc_store = True,
                # qqdump_dir_path = self.conf["qqdump_dir_path"],
                # vacuum_dir_path = "/mnt/ssd/vacuum-wiki-06-24.plus.align")

        convert(use_bloom_filters = True,
                align_doc_store = True,
                qqdump_dir_path = self.conf["qqdump_dir_path"],
                vacuum_dir_path = "/mnt/ssd/vacuum-reddit-07-02-bloom-5-0.0009-full")


def main():
    os.chdir("build")

    build_creator()
    build_convertor()

    index = VacuumWiki()
    # index.create_qq()
    index.convert_to_vacuum()


if __name__ == "__main__":
    main()

