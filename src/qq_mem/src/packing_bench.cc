#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>

#include "flash_iterators.h"
#include "test_helpers.h"
#include "utils.h"


void Bench() {
  std::vector<uint32_t> doc_ids;
  int num_docids = 100000;
  for (uint32_t i = 0; i < num_docids; i++) {
    doc_ids.push_back(i);
  }

  std::string path = "/tmp/tmp.docid";
  FileOffsetsOfBlobs file_offsets = DumpCozyBox(doc_ids, path, true);

  SkipList skip_list = CreateSkipListForDodId(
      GetSkipPostingPreDocIds(doc_ids), file_offsets.BlobOffsets());

  // Open the file
  utils::FileMap file_map;
  file_map.Open(path);

  // Read data by TermFreqIterator
  DocIdIterator iter((const uint8_t *)file_map.Addr(), &skip_list, num_docids);

  auto start = utils::now();
  for (uint32_t i = 0; i < num_docids; i++) {
    iter.SkipTo(i);
  }
  auto end = utils::now();

  std::cout << "Duration: " << utils::duration(start, end) << std::endl;

  file_map.Close();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  Bench();
}


