from pyreuse.fsutils import utils as fsutils
from pyreuse.helpers import *



class LineDocPool(object):
    def __init__(self, doc_path):
        self.fd = open(doc_path)

        # Get column names
        # The first line of the doc must be the header
        # Sample header:
        #   FIELDS_HEADER_INDICATOR###      doctitle        docdate body
        header = self.fd.readline()
        self.col_names = header.split("###")[1].split()

    def doc_iterator(self):
        for line in self.fd:
            yield self.line_to_dict(line)

    def line_to_dict(self, line):
        items = line.split("\t")
        return {k:v for k,v in zip(self.col_names, items)}


def setup_dev(devpath, mntpoint):
    fsutils.umount_wait(devpath)

    if "loop" in devpath:
        fsutils.makeLoopDevice(devpath, "/mnt/tmpfs", 1024*32)

    # shcmd("mkfs.ext4 -O ^has_journal -E lazy_itable_init=0,lazy_journal_init=0 {}".format(devpath))
    shcmd("mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 {}".format(devpath))
    prepare_dir(mntpoint)
    # shcmd("mount -o data=writeback {} {}".format(devpath, mntpoint))
    shcmd("mount {} {}".format(devpath, mntpoint))

