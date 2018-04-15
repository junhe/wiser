#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "utils.h"


TEST_CASE( "CozyBoxIterator", "[qqflash][cozy]" ) {
  std::vector<uint32_t> vec;
  int cnt = 300;
  for (uint32_t i = 0; i < cnt; i++) {
    vec.push_back(i);
  }

  std::string path = "/tmp/tmp.tf";
  FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);

  // Open the file
  utils::FileMap file_map(path);


  SECTION("Simple") {
    utils::PrintVec<off_t>(file_offsets.PackOffs());
    
    CozyBoxIterator iter((const uint8_t *)file_map.Addr());

    iter.GoToCozyEntry(0, 0);
    REQUIRE(iter.Value() == 0);
    REQUIRE(iter.CurBlobOffset() == 0);
  }

  SECTION("Advance() to the end") {
    CozyBoxIterator iter((const uint8_t *)file_map.Addr());
    iter.GoToCozyEntry(0, 0);
    
    for (uint32_t i = 0; i < cnt; i++) {
      REQUIRE(iter.Value() == i);
      iter.Advance();
    }
  }

  SECTION("Start from the last part of pack") {
    CozyBoxIterator iter((const uint8_t *)file_map.Addr());
    iter.GoToCozyEntry(0, PACK_SIZE - 10);
    
    for (uint32_t i = PACK_SIZE - 10; i < cnt; i++) {
      REQUIRE(iter.Value() == i);
      iter.Advance();
    }
  }

  SECTION("Start from the second packints") {
    CozyBoxIterator iter((const uint8_t *)file_map.Addr());
    iter.GoToCozyEntry(file_offsets.PackOffs()[1], 0);
    
    for (uint32_t i = PACK_SIZE; i < cnt; i++) {
      REQUIRE(iter.Value() == i);
      iter.Advance();
    }
  }

  SECTION("Start from the second packints, 2") {
    CozyBoxIterator iter((const uint8_t *)file_map.Addr());
    iter.GoToCozyEntry(file_offsets.PackOffs()[1], 2);
    
    for (uint32_t i = PACK_SIZE + 2; i < cnt; i++) {
      REQUIRE(iter.Value() == i);
      iter.Advance();
    }
  }


  SECTION("Start from the vints") {
    CozyBoxIterator iter((const uint8_t *)file_map.Addr());
    iter.GoToCozyEntry(file_offsets.VIntsOffs()[0], 0);
    
    for (uint32_t i = PACK_SIZE * 2; i < cnt; i++) {
      REQUIRE(iter.Value() == i);
      iter.Advance();
    }
  }


}


