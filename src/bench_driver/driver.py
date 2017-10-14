import os

import algs
from expbase import BenchRun, Experiment

from pyreuse import helpers

DOWNLOAD_DIR = "/mnt/sdc1/downloads"

class ExperimentWikiSmall(Experiment):
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "wikismall-trial-001"

    def conf(self, i):
        return {}

    def before(self):
        # download wiki dump
        helpers.prepare_dir(DOWNLOAD_DIR)
        with helpers.cd(DOWNLOAD_DIR):
            helpers.shcmd("wget -nc https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2")
            if not os.path.exists("enwiki-latest-pages-articles14.xml-p7697599p7744799"):
                helpers.shcmd("bunzip2 enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2")

        # create line doc
        benchrun = BenchRun(algs.CREATE_LINE_DOC)
        benchrun.run()

        # index line doc
        benchrun = BenchRun(algs.INDEX_LINE_DOC)
        benchrun.run()

    def treatment(self, conf):
        # benchrun = BenchRun(algs.CREATE_LINE_DOC)
        # benchrun.run()
        pass



def main():
    exp = ExperimentWikiSmall()
    exp.main()


if __name__ == "__main__":
    main()

