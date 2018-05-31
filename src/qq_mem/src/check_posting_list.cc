#include <glog/logging.h>
#include <gflags/gflags.h>

#include "general_config.h"

#include "types.h"
#include "engine_factory.h"
#include "qq_mem_engine.h"
#include "utils.h"

// This program checks if the sizes of posting lists match the doc freqs
// in the line doc.
//
// Input:
//  1. vacuum engine
//  2. term and doc freq 


struct CountEntry {
  std::string term;
  int doc_count;
};


class DocCountReader {
 public:
   DocCountReader(const std::string &path)
    :in_(path) {
      if (in_.good() == false) {
        throw std::runtime_error("File may not exist");
      }
   }

   bool NextEntry(CountEntry &entry) {
     std::string line;
     auto &ret = std::getline(in_, line);

     if (ret) {
       std::vector<std::string> items = utils::explode(line, ' ');
       entry.term = items[0];
       entry.doc_count = std::stoi(items[1]);

       return true;
     } else {
       return false;
     }
   }

 private:
   std::ifstream in_;
};


void CheckDocFreq() {
  std::unique_ptr<SearchEngineServiceNew> engine = 
    CreateSearchEngine("vacuum:vacuum_dump:/mnt/ssd/vacuum-05-23-wiki-2018May");
  engine->Load();
  std::cout << "Total Term Count: " << engine->TermCount() << std::endl;

  DocCountReader reader("/mnt/ssd/popular_terms_2018-may_wiki");
  CountEntry entry;

  int cnt = 0;
  while (reader.NextEntry(entry)) {
    // std::cout << entry.term << " : " << entry.doc_count << std::endl;
    
    SearchQuery query(TermList({entry.term}));
    auto result = engine->Search(query);
    if (result.doc_freqs[0] != entry.doc_count) {
      std::cout << "Not equal!! " 
        << "Term: " << entry.term 
        << "Pop terms count: " << entry.doc_count
        << "Posting list size: " << result.doc_freqs[0]
        << std::endl;
    }

    cnt++;
    if (cnt % 100000 == 0) 
      std::cout << "Finished " << cnt << std::endl;
  }
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  CheckDocFreq();
}


