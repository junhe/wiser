#include <iostream>
#include <cassert>

#include <glog/logging.h>

#include "intersect.h"
#include "utils.h"


int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  PostingList_Vec<Posting> pl01("hello");   
  for (int i = 0; i < 1000000; i++) {
    pl01.AddPosting(Posting(i, 1, Positions{28}));
  }

  PostingList_Vec<Posting> pl02("world");   
  for (int i = 0; i < 1000000; i++) {
    pl02.AddPosting(Posting(i, 1, Positions{28}));
  }

  std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};

  auto start = utils::now();
  std::vector<DocIdType> ret = intersect(lists);

  // for (auto d : ret) {
    // std::cout << d << ",";
  // }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  assert(ret.size() == 1000000);
  std::cout << "Took " << dur << " secs" << std::endl;
}
