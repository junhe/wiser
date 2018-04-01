import unittest
from pyreuse.apputils.fio import *
from pyreuse.macros import *
from pyreuse.helpers import parameter_combinations


baselist = [
                ("global", {
                    'ioengine'  : 'libaio',
                    'size'      : 4096,
                    }
                ),
                ("writer", {
                    'group_reporting': NOVALUE,
                    'numjobs'   : 1,
                    'rw'        : 'write'
                    }
                )
            ]


def read(expname, depth, bs):
    conf = JobConfig([
            ("global", {
                'ioengine'  : 'libaio',
                'runtime'   : 10,
                'filesize'  : 32*GB,
                'bs'        : bs,
                # 'filename_format'  : '$jobnum.$filenum',
                # 'directory' : '/mnt/ssd/fio-test-dir',
                'filename'  : '/mnt/ssd/fiofile',
                'direct'    : 1
                }
            ),
            (expname, {
                'group_reporting': NOVALUE,
                # 'numjobs'   : depth,
                'iodepth'   : depth,
                'rw'        : 'randread'
                }
            )
        ])

    conf_path = '/tmp/' + expname
    conf.save(conf_path)
    fio = Fio(conf_path = conf_path, result_dir = '/tmp/', to_json=True)
    fio.run()

    # shcmd("rm " + conf_path)
    shcmd("mv /tmp/fio.result.json.parsed /tmp/" + expname + ".result")


def get_expname(param):
    return "depth." + str(param['depth']) + ".n_pages." + str(param['n_pages'])

def main():
    parameter_dict = {
                    'depth'   : [1, 2, 4, 8, 16, 32, 64],
                    'n_pages' : [1, 2, 4, 8, 16, 32, 64]
                    }
    params_comb = parameter_combinations(parameter_dict)

    for param in params_comb:
        read(get_expname(param), param['depth'], param['n_pages'] * 4096)

    # for depth in [1, 2, 4, 8, 16, 32, 64]:
        # read(str(depth).zfill(5), depth)

    # for n_pages in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]:
        # read_size(str(n_pages).zfill(6), n_pages * 4096)

if __name__ == "__main__":
    main()






















