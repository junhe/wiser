import os

import algs
from expbase import BenchRun, Experiment

from pyreuse import helpers
from pyreuse.macros import *


class ExperimentWiki(Experiment):
    def __init__(self):
        self._n_treatments = 3
        self._exp_name = "wikipedia-lucene-001"

        self.DOWNLOAD_DIR = "/mnt/ssd/downloads"

        # self.work_dir = "/mnt/ssd/work-small-wiki"
        # helpers.shcmd("rm -rf " + self.work_dir)
        # self.download_url = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
        # self.origin_doc_name = "enwiki-latest-pages-articles14.xml-p7697599p7744799"
        # self.decompress_cmd = "bunzip2 enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2"
        # self.index_doc_count = "*"
        # self.search_count = 5
        # self.search_mem_size = None

        # self.work_dir = "/mnt/ssd/work-medium-wiki"
        #self.work_dir = "/mnt/fsonloop/work-medium-wiki"
        # #helpers.shcmd("rm -rf " + os.path.join(self.work_dir, "index"))
        # self.download_url = "https://dumps.wikimedia.org/enwiki/20171001/enwiki-20171001-pages-articles9.xml-p1791081p2336422.bz2"
        # self.origin_doc_name = "enwiki-20171001-pages-articles9.xml-p1791081p2336422"
        # self.decompress_cmd = "bunzip2 enwiki-20171001-pages-articles9.xml-p1791081p2336422.bz2"
        # self.index_doc_count = "*"
        # self.search_count =  100000
        # self.search_mem_size = None

    def conf(self, i):

        conf = {
            # Small wiki
            # "work_dir": "/mnt/ssd/work-small-wiki",
            # "download_url": "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2",
            # "origin_doc_name": "enwiki-latest-pages-articles14.xml-p7697599p7744799",
            # "decompress_cmd": "bunzip2 enwiki-latest-pages-articles14.xml-p7697599p7744799.bz2",
            # "index_doc_count": "1000",
            # "search_count": 100000,
            # "search_mem_size": None,
            # "query_maker": "org.apache.lucene.benchmark.byTask.feeds.WikiQueryMaker",
            # "wiki_query_log_path": "/mnt/ssd/downloads/wiki_QueryLog.clean",
            # "wiki_query_count": 10000,

            "work_dir": "/mnt/ssd/work-large-wiki-tmp",
            # "work_dir": "/mnt/fsonloop/work-large-wiki",
            "force_indexing": True,
            "download_url": "https://dumps.wikimedia.org/enwiki/20171001/enwiki-20171001-pages-articles.xml.bz2",
            "origin_doc_name": "enwiki-20171001-pages-articles.xml",
            "decompress_cmd": "bunzip2 enwiki-20171001-pages-articles.xml.bz2",
            "index_doc_count": 10**(i+3),
            "line_doc_path": "/mnt/ssd/work-large-wiki/linedoc",
            "search_count": 100000,
            "search_mem_size": None,
            "query_maker": "org.apache.lucene.benchmark.byTask.feeds.WikiQueryMaker",
            "wiki_query_log_path": "/mnt/ssd/downloads/wiki_QueryLog.clean",
            "wiki_query_count": 100000,
        }

        return conf

    def prepare_index(self, conf):
        helpers.prepare_dir(self.DOWNLOAD_DIR)
        helpers.prepare_dir(conf['work_dir'])

        self.download(conf)
        self.create_line_doc(conf)
        self.create_index(conf)
        self.prepare_wiki_query(conf)

    def prepare_wiki_query(self, conf):
        if conf['query_maker'].strip() == "org.apache.lucene.benchmark.byTask.feeds.WikiQueryMaker" and \
                os.path.exists(conf['wiki_query_log_path']) is False:
            # we need to download wiki_log
            dir_path = os.path.dirname(conf['wiki_query_log_path'])
            with helpers.cd(dir_path):
                helpers.shcmd("wget http://pages.cs.wisc.edu/~kanwu/querylog/wiki_QueryLog")

    def download(self, conf):
        with helpers.cd(self.DOWNLOAD_DIR):
            filename = conf['download_url'].split("/")[-1]
            print "------------", filename
            if not os.path.exists(filename) or not os.path.exists(conf['origin_doc_name']):
                helpers.shcmd("wget -nc {}".format(conf['download_url']))

            self.origin_doc_path = os.path.join(self.DOWNLOAD_DIR, conf['origin_doc_name'])
            if not os.path.exists(conf['origin_doc_name']):
                helpers.shcmd(conf['decompress_cmd'])

    def create_line_doc(self, conf):
        # skip if exists
        if os.path.exists(conf['line_doc_path']):
            return

        benchrun = BenchRun(algs.CREATE_LINE_DOC(
            docs_file = self.origin_doc_path,
            line_file_out = conf['line_doc_path']
            ))
        benchrun.run()

    def create_index(self, conf):
        # skip if exists
        index_path = os.path.join(conf['work_dir'], 'index')

        if conf['force_indexing'] is True:
            helpers.shcmd("rm -rf {}".format(index_path))

        if os.path.exists(index_path):
            return

        benchrun = BenchRun(algs.INDEX_LINE_DOC(
            docs_file = conf['line_doc_path'],
            work_dir = conf['work_dir'],
            index_doc_count = conf['index_doc_count']
            ))
        benchrun.run()

    def before(self):
        pass

    def beforeEach(self, conf):
        self.prepare_index(conf)

        helpers.shcmd("dropcache")

    def treatment(self, conf):
        benchrun = BenchRun(
            algorithm_text = algs.REUTER_SEARCH(
                docs_file = "/tmp/",
                work_dir = conf['work_dir'],
                search_count = conf['search_count'],
                query_maker = conf['query_maker'],
                wiki_query_log_path = conf['wiki_query_log_path'],
                wiki_query_count = conf['wiki_query_count']
                ),
            mem_size = conf['search_mem_size']
            )
        benchrun.run()


def main():
    exp = ExperimentWiki()
    exp.main()



if __name__ == "__main__":
    main()

