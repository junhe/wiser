import os
import time

import algs
import config

from pyreuse.helpers import *
from pyreuse.sysutils.cgroup import Cgroup

class Experiment(object):
    """
    internal properties that can be used:
        _n_treatments
        _exp_name
        _resultdir
        _subexpdir
    """
    def __init__(self):
        self._n_treatments = 1
        self._exp_name = "default-exp-name"

    def _before(self):
        # properties with _name is read only, and used internally by
        # this class
        self._resultdir = os.path.join("/tmp/results/", self._exp_name)

    def before(self):
        pass

    def _beforeEach(self, conf):
        # setup subexpdir
        self._subexpdir = os.path.join(self._resultdir, str(time.time()))
        prepare_dir(self._subexpdir)

    def beforeEach(self, conf):
        pass

    def _afterEach(self, conf):
        pass

    def afterEach(self, conf):
        pass

    def _after(self):
        pass

    def after(self):
        pass

    def conf(self, i):
        pass

    def treatment(self, conf):
        pass

    def run_treatment(self, i):
        conf = self.conf(i)

        self._beforeEach(conf)
        self.beforeEach(conf)

        self.treatment(conf)

        self._afterEach(conf)
        self.afterEach(conf)

    def main(self):
        self._before()
        self.before()
        for i in range(self._n_treatments):
            self.run_treatment(i)
        self._after()
        self.after()


class BenchRun(object):
    """
    One run of the Lucene benchmark
    """
    def __init__(self, algorithm_text, mem_size = None):
        self.algorithm_text = algorithm_text
        self.bench_dir = os.path.join(config.LUCENEN_SRC, "benchmark")
        self.alg_path = "/tmp/flashsearch.benchrun.alg"
        self.mem_size = mem_size

    def run(self):
        self._before()
        self._run()
        self._after()

    def _before(self):
        with open(self.alg_path, "w") as f:
            f.write(self.algorithm_text)

    def _run(self):
        with cd(self.bench_dir):
            cmd = "ant run-task -Dtask.alg={}".format(self.alg_path)

            if self.mem_size is None:
                shcmd(cmd)
            else:
                cg = Cgroup(name='charlie', subs='memory')
                cg.set_item('memory', 'memory.limit_in_bytes', self.mem_size)
                p = cg.execute(shlex.split(cmd))
                p.wait()

    def _after(self):
        shcmd("rm {}".format(self.alg_path))






