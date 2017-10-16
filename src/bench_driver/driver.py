import os

import algs
from expbase import BenchRun, Experiment

from pyreuse import helpers
from pyreuse.macros import *


class ExperimentWikiSmall(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "wikismall-trial-001"

        self.DOWNLOAD_DIR = "/mnt/ssd/downloads"

        # self.work_dir = "/mnt/ssd/work-small-wiki"
        #helpers.shcmd("rm -rf " + self.work_dir)
        # self.download_url = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
        # self.origin_doc_name = "enwiki-latest-pages-articles14.xml-p7697599p7744799"
        # self.decompress_cmd = "bunzip2 enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
        # self.index_doc_count = 5000
        # self.search_count = 1000
        # self.search_mem_size = 256*MB

        #there are 1520000 reconds
        self.work_dir = "/mnt/ssd/work-medium-wiki"
        # self.work_dir = "/mnt/fsonloop/work-medium-wiki"
        self.download_url = "https://dumps.wikimedia.org/enwiki/20171001/enwiki-20171001-pages-articles9.xml-p1791081p2336422.bz2"
        self.origin_doc_name = "enwiki-20171001-pages-articles9.xml-p1791081p2336422"
        self.decompress_cmd = "bunzip2 enwiki-20171001-pages-articles9.xml-p1791081p2336422.bz2"
        self.index_doc_count = 500000
        self.search_count =  10000
        self.search_mem_size = 64*MB

        # self.work_dir = "/mnt/ssd/work-large-wiki"
        # self.download_url = "https://dumps.wikimedia.org/enwiki/20171001/enwiki-20171001-pages-articles.xml.bz2"
        # self.origin_doc_name = "enwiki-20171001-pages-articles.xml"
        # self.decompress_cmd = "bunzip2 enwiki-20171001-pages-articles.xml.bz2"
        # self.search_count = 1000
        # self.index_doc_count = 1000

    def conf(self, i):
        return {}

    def prepare_index(self):
        helpers.prepare_dir(self.DOWNLOAD_DIR)
        helpers.prepare_dir(self.work_dir)

        self.download()
        self.create_line_doc()
        self.create_index()

    def download(self):
        with helpers.cd(self.DOWNLOAD_DIR):
            helpers.shcmd("wget -nc {}".format(self.download_url))

            self.origin_doc_path = os.path.join(self.DOWNLOAD_DIR, self.origin_doc_name)
            if not os.path.exists(self.origin_doc_name):
                helpers.shcmd(self.decompress_cmd)

    def create_line_doc(self):
        self.line_file_out = os.path.join(self.work_dir, "linedoc")

        # skip if exists
        if os.path.exists(self.line_file_out):
            return

        benchrun = BenchRun(algs.CREATE_LINE_DOC(
            docs_file = self.origin_doc_path,
            line_file_out = self.line_file_out
            ))
        benchrun.run()

    def create_index(self):
        # skip if exists
        if os.path.exists(os.path.join(self.work_dir, 'index')):
            return

        benchrun = BenchRun(algs.INDEX_LINE_DOC(
            docs_file = self.line_file_out,
            work_dir = self.work_dir,
            index_doc_count = self.index_doc_count
            ))
        benchrun.run()

    def before(self):
        self.prepare_index()

        helpers.shcmd("dropcache")

    def treatment(self, conf):
        benchrun = BenchRun(
            algorithm_text = algs.REUTER_SEARCH(
                docs_file = "/tmp/",
                work_dir = self.work_dir,
                search_count = self.search_count
                ),
            mem_size = self.search_mem_size
            )
        benchrun.run()


def main():
    exp = ExperimentWikiSmall()
    exp.main()



if __name__ == "__main__":
    main()

