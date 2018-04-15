#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "utils.h"


TEST_CASE( "CozyBoxIterator", "[qqflash][cozy]" ) {
  std::vector<uint32_t> vec;
  for (uint32_t i = 0; i < 300; i++) {
    vec.push_back(i);
  }

  std::string path = "/tmp/tmp.tf";
  FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);

  // Open the file
  utils::FileMap file_map(path);

  CozyBoxIterator iter((const uint8_t *)file_map.Addr());


}


