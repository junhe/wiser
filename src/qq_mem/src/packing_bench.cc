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


const int n_packs = 1000000;
const int LIMIT = 10000000;

void BenchLittlePackedInts() {
  std::vector<std::string> data; 
  srand(0);
  for (int i = 0; i < n_packs; i++) {
    LittlePackedIntsWriter writer;
    for (int j = 0; j < PACK_ITEM_CNT; j++) {
      writer.Add(rand() % LIMIT);
    }
    data.push_back(writer.Serialize());
  }

  
  auto start = utils::now();
  LittlePackedIntsReader reader;
  for (int i = 0; i < n_packs; i++) {
    uint8_t *buf = (uint8_t *)data[i].data();
    reader.Reset(buf);
    reader.DecodeToCache();
    for (int j = 0; j < PACK_ITEM_CNT; j++) {
      reader.Get(j);
    }
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);
  std::cout << "Duration: " <<  dur << std::endl;
}

void BenchPackedInts() {
  std::vector<std::string> data; 
  srand(0);
  for (int i = 0; i < n_packs; i++) {
    PackedIntsWriter writer;
    for (int j = 0; j < PACK_ITEM_CNT; j++) {
      writer.Add(rand() % LIMIT);
    }
    data.push_back(writer.Serialize());
  }

  
  auto start = utils::now();
  PackedIntsReader reader;
  for (int i = 0; i < n_packs; i++) {
    uint8_t *buf = (uint8_t *)data[i].data();
    reader.Reset(buf);
    for (int j = 0; j < PACK_ITEM_CNT; j++) {
      reader.Get(j);
    }
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);
  std::cout << "Duration: " <<  dur << std::endl;
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  BenchLittlePackedInts();
  BenchPackedInts();
}


