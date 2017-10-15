import os

import algs
from expbase import BenchRun, Experiment

from pyreuse import helpers


class ExperimentWikiSmall(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "wikismall-trial-001"

        self.DOWNLOAD_DIR = "/mnt/ssd/downloads"
        # self.work_dir = "/mnt/sdc1/work3"
        self.work_dir = "/mnt/ssd/work-2"


    def conf(self, i):
        return {}

    def prepare_index(self):
        helpers.prepare_dir(self.DOWNLOAD_DIR)
        helpers.prepare_dir(self.work_dir)

        self.download_small_wiki()
        # self.download_medium_wiki()
        # self.download_large_wiki()
        self.create_line_doc()
        self.create_index()

    def download_small_wiki(self):
        self.download_url = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
        self.origin_doc_name = "enwiki-latest-pages-articles14.xml-p7697599p7744799"
        self.decompress_cmd = "bunzip2 enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"

        with helpers.cd(self.DOWNLOAD_DIR):
            helpers.shcmd("wget -nc {}".format(self.download_url))

            self.origin_doc_path = os.path.join(self.DOWNLOAD_DIR, self.origin_doc_name)
            if not os.path.exists(self.origin_doc_name):
                helpers.shcmd(self.decompress_cmd)

    def download_medium_wiki(self):
        with helpers.cd(self.DOWNLOAD_DIR):
            helpers.shcmd("wget -nc https://dumps.wikimedia.org/enwiki/20171001/enwiki-20171001-pages-articles9.xml-p1791081p2336422.bz2")

            self.origin_doc_name = "enwiki-20171001-pages-articles9.xml-p1791081p2336422"
            self.origin_doc_path = os.path.join(self.DOWNLOAD_DIR, self.origin_doc_name)

            if not os.path.exists(self.origin_doc_name):
                helpers.shcmd("bunzip2 enwiki-20171001-pages-articles9.xml-p1791081p2336422.bz2")

    def download_large_wiki(self):
        with helpers.cd(self.DOWNLOAD_DIR):
            helpers.shcmd("wget -nc https://dumps.wikimedia.org/enwiki/20171001/enwiki-20171001-pages-articles.xml.bz2")

            self.origin_doc_name = "enwiki-20171001-pages-articles.xml"
            self.origin_doc_path = os.path.join(self.DOWNLOAD_DIR, self.origin_doc_name)

            if not os.path.exists(self.origin_doc_name):
                helpers.shcmd("bunzip2 enwiki-20171001-pages-articles.xml")

    def create_line_doc(self):
        self.line_file_out = os.path.join(self.work_dir, "linedoc")
        benchrun = BenchRun(algs.CREATE_LINE_DOC(
            docs_file = self.origin_doc_path,
            line_file_out = self.line_file_out
            ))
        benchrun.run()

    def create_index(self):
        print "++++++++++++++++++++++ Indexing Line Doc +++++++++++++++++++++"
        benchrun = BenchRun(algs.INDEX_LINE_DOC(
            docs_file = self.line_file_out,
            work_dir = self.work_dir
            ))
        benchrun.run()

    def before(self):
        self.prepare_index()

    def treatment(self, conf):
        benchrun = BenchRun(algs.REUTER_SEARCH(
            docs_file = "/tmp/",
            work_dir = self.work_dir
            ))
        benchrun.run()



def main():
    exp = ExperimentWikiSmall()
    exp.main()



if __name__ == "__main__":
    main()

