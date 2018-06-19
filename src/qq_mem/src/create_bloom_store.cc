#include <glog/logging.h>
#include <gflags/gflags.h>

#include "utils.h"
#include "engine_loader.h"
#include "bloom_filter.h"

DEFINE_string(line_doc_path, "", "path of the line doc");
DEFINE_string(dump_dir_path, "", "path of the directory to dump to");


class BloomShell {
 public:
  void Load(const std::string &line_doc_path) {
    LineDocParserBiBloom parser(line_doc_path, 100000000);

    DocInfo doc_info;
    while (parser.Pop(&doc_info)) {
      AddDocument(doc_info); 
      if (parser.Count() % 10000 == 0) {
        std::cout << "Indexed " << parser.Count() << " documents" << std::endl;
      }
    }
  }

  void AddDocument(const DocInfo doc_info) {
    int doc_id = NextDocId();

    bloom_store_begin_.Add(doc_id, doc_info.GetTokens(), 
        doc_info.GetPhraseBegins());
    bloom_store_end_.Add(doc_id, doc_info.GetTokens(), 
        doc_info.GetPhraseEnds());
  }

  void Dump(const std::string &dir_path) {
    std::cout << "Dumping bloom filters with index to " 
      << dir_path << " ..." << std::endl;

    utils::PrepareDir(dir_path);

    bloom_store_begin_.SerializeWithIndex(
        dir_path + "/bloom_begin.meta",
        dir_path + "/bloom_begin.index",
        dir_path + "/bloom_begin.store");
    bloom_store_end_.SerializeWithIndex(
        dir_path + "/bloom_end.meta",
        dir_path + "/bloom_end.index",
        dir_path + "/bloom_end.store");
  }

 private:
  int NextDocId() {
    return next_doc_id_++;
  }

  BloomFilterStore bloom_store_begin_;
  BloomFilterStore bloom_store_end_;

  int next_doc_id_ = 0;
};



int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  BloomShell shell;
  shell.Load(FLAGS_line_doc_path);
  shell.Dump(FLAGS_dump_dir_path);
}



