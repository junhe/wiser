#include "bloom_filter.h"


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 2; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  BloomFilterStore store;
  std::cout << "Deserializing bloom filter store..." << std::endl;
  store.Deserialize("/mnt/ssd/qq-06-08-with-bloom-empty-set/bloom_filter.dump");
}


