#include <glog/logging.h>
#include <gflags/gflags.h>

#include "general_config.h"

#include "engine_factory.h"
#include "qq_mem_engine.h"
#include "utils.h"

// This program reads a line doc, convert them to qq memory dump format

DEFINE_string(line_doc_path, "", "path of the line doc");
DEFINE_string(dump_dir_path, "", "path of the directory to dump to");
DEFINE_int32(bloom_entries, 5, "number of entries in a bloom filter");
DEFINE_double(bloom_ratio, 0.001, "false positive ratio in a bloom filter");
DEFINE_int32(n_lines, 1, "number of lines to read from line doc");
DEFINE_string(dump_type, "", "inverted/bloom_begin/bloom_end");

void CheckArgs() {
  if (FLAGS_line_doc_path == "") {
    LOG(FATAL) << "Arg line_doc_path is not set";
  }

  if (FLAGS_dump_dir_path == "") {
    LOG(FATAL) << "Arg dump_dir_path is not set";
  }
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  CheckArgs();

  if (FLAGS_dump_type == "inverted") {
    std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
        "qq_mem_compressed");
    engine->LoadLocalDocuments(FLAGS_line_doc_path, FLAGS_n_lines, "WITH_POSITIONS");
    engine->Serialize(FLAGS_dump_dir_path);
    engine.release();
  } else if (FLAGS_dump_type == "bloom_begin") {
    BloomBeginDumper bloom_dumper(FLAGS_bloom_ratio, FLAGS_bloom_entries);
    bloom_dumper.Load(FLAGS_line_doc_path, FLAGS_n_lines);
    bloom_dumper.Dump(FLAGS_dump_dir_path);
  } else if (FLAGS_dump_type == "bloom_end") {
    BloomEndDumper bloom_dumper(FLAGS_bloom_ratio, FLAGS_bloom_entries);
    bloom_dumper.Load(FLAGS_line_doc_path, FLAGS_n_lines);
    bloom_dumper.Dump(FLAGS_dump_dir_path);
  } else {
    LOG(FATAL) << "dump_type " << FLAGS_dump_type << " is not supported";
  }
}

