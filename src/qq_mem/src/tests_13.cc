#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "utils.h"


TEST_CASE( "CozyBoxIterator", "[qqflash][cozy]" ) {
  SECTION("Only some vints") {
    std::vector<uint32_t> vec;
    int cnt = 8;
    for (uint32_t i = 0; i < cnt; i++) {
      vec.push_back(i);
    }

    std::string path = "/tmp/tmp.tf";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);

    // Open the file
    utils::FileMap file_map(path);

    SECTION("Advance() to the end") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(0, 0);
      
      for (uint32_t i = 0; i < cnt; i++) {
        REQUIRE(iter.Value() == i);
        iter.Advance();
      }
    }
  }

 
  SECTION("2 packs, some vints") {
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
}

TEST_CASE( "Position Bag iterator", "[qqflash][pos]" ) {
  // Build term frequency iterator
  std::vector<uint32_t> vec;
  int n_postings = 300;
  for (uint32_t i = 0; i < n_postings; i++) {
    vec.push_back(3);
  }

  std::string path = "/tmp/tmp.tf";
  FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
  SkipList skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

  // Open the file
  utils::FileMap file_map(path);

  // Read data by TermFreqIterator
  TermFreqIterator iter((const uint8_t *)file_map.Addr(), skip_list);

  SECTION("Check if the TF iterator works") {
    for (uint32_t i = 0; i < n_postings; i++) {
      iter.SkipTo(i);
      REQUIRE(iter.Value() == 3);
    }
  }

  SECTION("Position iterator") {
    std::vector<uint32_t> positions;
    for (uint32_t i = 0; i < n_postings * 3; i++) {
      positions.push_back(i);
    }

    std::string path = "/tmp/tmp.pos";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
    std::vector<off_t> blob_offs = file_offsets.BlobOffsets();
    int n_intervals = n_postings / PACK_SIZE;
    
    std::vector<off_t> blob_offs_of_pos_bags; // for skip postings only
    std::vector<int> in_blob_indexes;
    for (int i = 0; i < n_intervals; i++) {
      blob_offs_of_pos_bags.push_back(blob_offs[i*3]);
      in_blob_indexes.push_back(0);
    }

    SkipList skip_list = CreateSkipListForPosition(
        blob_offs_of_pos_bags, in_blob_indexes);

    // Open the file
    utils::FileMap file_map(path);
  }
}

/*
TEST_CASE( "Position Bag iterator", "[qqflash][pos]" ) {
  SECTION("Simple unrealistic sequential positions") {
    std::vector<uint32_t> positions;
    int num_positions = 300;
    for (uint32_t i = 0; i < num_positions; i++) {
      positions.push_back(i);
    }

    std::string path = "/tmp/tmp.docid";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(positions, path, false);

    SkipList skip_list = CreateSkipListForDodId(
        GetSkipPostingPreDocIds(doc_ids), file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    DocIdIterator iter((const uint8_t *)file_map.Addr(), skip_list, num_docids);

    SECTION("Skip one by one") {
      for (uint32_t i = 0; i < num_docids; i++) {
        iter.SkipTo(i);
        // std::cout << "i: " << iter.Value() << std::endl;
        REQUIRE(iter.Value() == i);
      }

      file_map.Close();
    }

    SECTION("Advance()") {
      for (uint32_t i = 0; i < num_docids; i++) {
        // std::cout << "i: " << iter.Value() << std::endl;
        REQUIRE(iter.Value() == i);
        REQUIRE(iter.IsEnd() == false);
        iter.Advance();
      }
      REQUIRE(iter.IsEnd() == true);

      file_map.Close();
    }
  }
}

*/







