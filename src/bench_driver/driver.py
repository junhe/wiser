import os

import algs
from expbase import BenchRun, Experiment

from pyreuse import helpers

DOWNLOAD_DIR = "/mnt/sdc1/downloads"
WORK_DIR = "/mnt/sdc1/work"

class ExperimentWikiSmall(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "wikismall-trial-001"

    def conf(self, i):
        return {}

    def before(self):
        helpers.prepare_dir(DOWNLOAD_DIR)
        helpers.prepare_dir(WORK_DIR)

        # download wiki dump
        with helpers.cd(DOWNLOAD_DIR):
            helpers.shcmd("wget -nc https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2")
            if not os.path.exists("enwiki-latest-pages-articles14.xml-p7697599p7744799"):
                helpers.shcmd("bunzip2 enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2")

        # create line doc
        print "++++++++++++++++++++++ Creating Line Doc +++++++++++++++++++++"
        benchrun = BenchRun(algs.CREATE_LINE_DOC(
            line_file_out = os.path.join(WORK_DIR, "enwiki.linedoc"),
            docs_file = os.path.join(DOWNLOAD_DIR, "enwiki-latest-pages-articles14.xml-p7697599p7744799")
            ))
        benchrun.run()

        print "++++++++++++++++++++++ Indexing Line Doc +++++++++++++++++++++"
        benchrun = BenchRun(algs.INDEX_LINE_DOC(
            docs_file = os.path.join(WORK_DIR, "enwiki.linedoc")
            ))
        benchrun.run()

    def treatment(self, conf):
        benchrun = BenchRun(algs.REUTER_SEARCH(
            docs_file = "/tmp/",
            work_dir = "work"
            ))
        benchrun.run()



def main():
    exp = ExperimentWikiSmall()
    exp.main()



if __name__ == "__main__":
    main()

