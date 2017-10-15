import os

import algs
from expbase import BenchRun, Experiment

from pyreuse import helpers


class ExperimentWikiSmall(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "wikismall-trial-001"

        self.DOWNLOAD_DIR = "/mnt/sdc1/downloads"
        self.WORK_DIR = "/mnt/sdc1/work3"

        self.line_file_out = os.path.join(self.WORK_DIR, "enwiki.linedoc")
        self.origin_doc_name = "enwiki-latest-pages-articles14.xml-p7697599p7744799"
        self.origin_doc_path = os.path.join(self.DOWNLOAD_DIR, self.origin_doc_name)

    def conf(self, i):
        return {}

    def prepare_index(self):
        helpers.prepare_dir(self.DOWNLOAD_DIR)
        helpers.prepare_dir(self.WORK_DIR)

        self.download()
        self.create_line_doc()
        self.create_index()

    def download(self):
        with helpers.cd(self.DOWNLOAD_DIR):
            helpers.shcmd("wget -nc https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2")
            if not os.path.exists(self.origin_doc_name):
                helpers.shcmd("bunzip2 enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2")

    def create_line_doc(self):
        print "++++++++++++++++++++++ Creating Line Doc +++++++++++++++++++++"
        benchrun = BenchRun(algs.CREATE_LINE_DOC(
            docs_file = self.origin_doc_path,
            line_file_out = self.line_file_out
            ))
        benchrun.run()

    def create_index(self):
        print "++++++++++++++++++++++ Indexing Line Doc +++++++++++++++++++++"
        benchrun = BenchRun(algs.INDEX_LINE_DOC(
            docs_file = self.line_file_out,
            work_dir = self.WORK_DIR
            ))
        benchrun.run()

    def before(self):
        self.prepare_index()

    def treatment(self, conf):
        benchrun = BenchRun(algs.REUTER_SEARCH(
            docs_file = "/tmp/",
            work_dir = self.WORK_DIR
            ))
        benchrun.run()



def main():
    exp = ExperimentWikiSmall()
    exp.main()



if __name__ == "__main__":
    main()

