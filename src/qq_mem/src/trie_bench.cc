#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>
#include <random>


#include <gperftools/profiler.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "vacuum_engine.h"


DEFINE_string(term_path, "", "path of the file with term and count");



class DocCountReader {
 public:
  DocCountReader(const std::string &path)
    :in_(path) {
      if (in_.good() == false) {
        throw std::runtime_error("File may not exist");
      }
    }

  bool NextEntry(std::string &entry) {
    std::string line;
    auto &ret = std::getline(in_, line);
    utils::trim(line);
    // std::cout << "line:" << line << std::endl;

    if (ret) {
      if (line == "") {
        entry == ""; 
      } else {
        std::vector<std::string> items = utils::explode(line, ' ');
        entry = items[0];
      }

      return true;
    } else {
      return false;
    }
  }

 private:
  std::ifstream in_;
};


void LoadTrie(TermTrieIndex *index, const std::string path) {
  DocCountReader reader(path);

  int cnt = 0;
  std::string term;
  while (reader.NextEntry(term)) {
    if (term == "") 
      std::cout << "Skipped one empty term" << std::endl;
    // std::cout << "Adding '" << term << "'" << std::endl;
    index->Add(term, rand());
 
    cnt++;
    if (cnt % 1000000 == 0) {
      std::cout << "Finished " << cnt << std::endl;
      // break;
    }
  }

  std::cout << "size: " << index->NumTerms() << std::endl;
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  TermTrieIndex index;
  std::cout << "flag:" << FLAGS_term_path << std::endl;
  std::vector<std::string> paths = utils::explode(FLAGS_term_path, ':');
  for (auto &path : paths) {
    std::cout << path << std::endl;
    LoadTrie(&index, path);
  }

  std::cout << "Final size: " << index.NumTerms() << std::endl;
  utils::sleep(1000);
}


