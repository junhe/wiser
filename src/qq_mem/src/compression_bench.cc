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
#include "posting_message.pb.h"


typedef RankingPostingWithOffsets PostingWO;

// Next() will generate a posting
class PostingGenerator {
 public:
  PostingGenerator() {
    srand(1);
  }

  PostingWO Next() {
    int cur_doc_id = next_doc_id_;
    next_doc_id_ += Rand(1, 100);
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

  int Rand(int low, int high) {
    return low + rand() % (high - low);
  }

  int Rand(int high) {
    return rand() % high;
  }

 private:
  int next_doc_id_ = 0;
};

void add_to_pb_posting(posting_message::PostingList *postinglist_pb,
    PostingWO posting) {
  posting_message::Posting *pb_posting = postinglist_pb->add_postings();
  pb_posting->set_docid(posting.GetDocId());
  pb_posting->set_term_frequency(posting.GetTermFreq());

  const OffsetPairs *from_pairs = posting.GetOffsetPairs();
  for (auto from_pair : *from_pairs) {
    posting_message::Offset *pair = pb_posting->add_positions();
    pair->set_start_offset(std::get<0>(from_pair));
    pair->set_end_offset(std::get<1>(from_pair));
  }
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  PostingGenerator gen;
  PostingList_Vec<PostingWO> postinglist_vec("hello");
  posting_message::PostingList postinglist_pb;

  for(int i = 0; i < 1000000; i++) {
    auto posting = gen.Next();
    // std::cout << posting.ToString() << std::endl;

    // Add to Vec
    // postinglist_vec.AddPosting(posting);

    // Add to Protobuf
    add_to_pb_posting(&postinglist_pb, posting);
  }

  std::cout << "Size of posting list (vec): " << postinglist_vec.Size() << std::endl;
  std::cout << "Size of posting list (pb) : " << postinglist_pb.postings_size() << std::endl;

  std::cout << "Done, just waiting to be killed..." << std::endl;

  utils::sleep(10000);
}


