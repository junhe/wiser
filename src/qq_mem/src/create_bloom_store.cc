#include <glog/logging.h>
#include <gflags/gflags.h>

#include "utils.h"
#include "engine_loader.h"
#include "bloom_filter.h"

DEFINE_string(line_doc_path, "", "path of the line doc");
DEFINE_string(dump_dir_path, "", "path of the directory to dump to");


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  BloomDumper shell;
  shell.Load(FLAGS_line_doc_path);
  shell.Dump(FLAGS_dump_dir_path);
}



