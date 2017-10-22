from pyreuse.fsutils import utils as fsutils
from pyreuse.helpers import *


def setup_dev(devpath, mntpoint):
    fsutils.umount_wait(devpath)

    if "loop" in devpath:
        fsutils.makeLoopDevice(devpath, "/mnt/tmpfs", 1024*32)

    # shcmd("mkfs.ext4 -O ^has_journal -E lazy_itable_init=0,lazy_journal_init=0 {}".format(devpath))
    shcmd("mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 {}".format(devpath))
    prepare_dir(mntpoint)
    # shcmd("mount -o data=writeback {} {}".format(devpath, mntpoint))
    shcmd("mount {} {}".format(devpath, mntpoint))

