#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>

#define NDEBUG
#include <glog/logging.h>

#include "qq_engine.h"
#include "qq_mem_uncompressed_engine.h"
#include "utils.h"
#include "general_config.h"
#include "query_pool.h"


typedef RankingPostingWithOffsets PostingWO;

// Next() will generate a posting
class PostingGenerator {
 public:
  PostingGenerator() {
    srand(1);
  }

  PostingWO Next() {
    int cur_doc_id = next_doc_id_;
    next_doc_id_ += Rand(100);
    int term_frequency = Rand(100);
    OffsetPairs offset_pairs = GeneratePairs();
    
    PostingWO posting(cur_doc_id, term_frequency, offset_pairs);

    return posting;
  }

  OffsetPairs GeneratePairs() {
    OffsetPairs pairs;

    int n_pairs = Rand(100);
    int cur_offset = Rand(2000);
    for( int i = 0; i < n_pairs; i++) {
      int off1 = cur_offset + Rand(100);  
      int off2 = off1 + Rand(10);  

      OffsetPair pair = std::make_tuple(off1, off2);
      pairs.push_back(pair);

      cur_offset = off2;
    }

    return pairs;
  }

  int Rand(int limit) {
    return rand() % limit;
  }

 private:
  int next_doc_id_ = 0;
};


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  PostingGenerator gen;
  PostingList_Vec<PostingWO> postinglist_vec("hello");
  for(int i = 0; i < 10; i++) {
    auto posting = gen.Next();
    std::cout << posting.ToString() << std::endl;
    postinglist_vec.AddPosting(posting);
  }


  utils::sleep(10000);
}


