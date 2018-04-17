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


std::vector<uint32_t> GoodPositions(int posting_bag) {
  std::vector<uint32_t> ret;
  uint32_t prev = 0;
  for (uint32_t delta = posting_bag * 3; delta < (posting_bag + 1) * 3; delta++) {
    uint32_t pos = prev + delta;
    ret.push_back(pos);
    prev = pos; 
  }

  return ret;
}

std::vector<uint32_t> GoodOffsets(int posting_bag) {
  std::vector<uint32_t> ret;
  uint32_t prev = 0;
  for (uint32_t delta = posting_bag * 3 * 2; delta < (posting_bag + 1) * 3 * 2; delta++) {
    uint32_t off = prev + delta;
    ret.push_back(off);
    prev = off; 
  }

  return ret;
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
  SkipList tf_skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

  // Open the file
  utils::FileMap file_map(path);

  // Read data by TermFreqIterator
  TermFreqIterator tf_iter((const uint8_t *)file_map.Addr(), &tf_skip_list);

  SECTION("Check if the TF iterator works") {
    for (uint32_t i = 0; i < n_postings; i++) {
      tf_iter.SkipTo(i);
      REQUIRE(tf_iter.Value() == 3);
    }
  }

  SECTION("Position iterator") {
    std::vector<uint32_t> positions;
    for (uint32_t i = 0; i < n_postings * 3; i++) {
      positions.push_back(i);
    }

    std::string path = "/tmp/tmp.pos";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(positions, path, false);
    std::vector<off_t> blob_offs = file_offsets.BlobOffsets();
    int n_intervals = (n_postings + PACK_SIZE - 1) / PACK_SIZE;
    
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

    // Initialize pos iterator
    PositionPostingBagIterator 
      pos_iter((const uint8_t*)file_map.Addr(), &skip_list, tf_iter);

    SECTION("Very simple") {
      pos_iter.SkipTo(0);

      InBagPositionIterator in_bag_iter = pos_iter.InBagPositionBegin();

      uint32_t prev = 0;
      for (int i = 0; i < 3; i++) {
        REQUIRE(in_bag_iter.IsEnd() == false);
        uint32_t pos = in_bag_iter.Pop();
        uint32_t good_pos = prev + i;
        REQUIRE(pos == good_pos);
        prev = good_pos;
      }
      REQUIRE(in_bag_iter.IsEnd() == true);
    }

    SECTION("Very simple 2") {
      int posting_bag = 1;
      pos_iter.SkipTo(posting_bag);

      auto good_positions = GoodPositions(posting_bag);

      InBagPositionIterator in_bag_iter = pos_iter.InBagPositionBegin();

      for (int i = 0; i < 3; i++) {
        REQUIRE(in_bag_iter.IsEnd() == false);
        uint32_t pos = in_bag_iter.Pop();
        REQUIRE(pos == good_positions[i]);
      }
      REQUIRE(in_bag_iter.IsEnd() == true);
    }

    SECTION("Go to the last bag") {
      int posting_bag = n_postings - 1;
      pos_iter.SkipTo(posting_bag);

      auto good_positions = GoodPositions(posting_bag);

      InBagPositionIterator in_bag_iter = pos_iter.InBagPositionBegin();

      for (int i = 0; i < 3; i++) {
        REQUIRE(in_bag_iter.IsEnd() == false);
        uint32_t pos = in_bag_iter.Pop();
        REQUIRE(pos == good_positions[i]);
      }
      REQUIRE(in_bag_iter.IsEnd() == true);
    }

    SECTION("Go to bag in the middle") {
      int posting_bag = n_postings / 2;
      pos_iter.SkipTo(posting_bag);

      InBagPositionIterator in_bag_iter = pos_iter.InBagPositionBegin();
      auto good_positions = GoodPositions(posting_bag);

      for (int i = 0; i < 3; i++) {
        REQUIRE(in_bag_iter.IsEnd() == false);
        uint32_t pos = in_bag_iter.Pop();
        REQUIRE(pos == good_positions[i]);
      }
      REQUIRE(in_bag_iter.IsEnd() == true);
    }

    SECTION("Skip multiple times") {
      for (int i = 0; i < n_postings; i++) {
        pos_iter.SkipTo(i);

        InBagPositionIterator in_bag_iter = pos_iter.InBagPositionBegin();
        auto good_positions = GoodPositions(i);

        for (int i = 0; i < 3; i++) {
          REQUIRE(in_bag_iter.IsEnd() == false);
          uint32_t pos = in_bag_iter.Pop();
          REQUIRE(pos == good_positions[i]);
        }
        REQUIRE(in_bag_iter.IsEnd() == true);
      }
    }

    SECTION("Skip multiple times with strides") {
      for (int i = 0; i < n_postings; i+=7) {
        pos_iter.SkipTo(i);

        InBagPositionIterator in_bag_iter = pos_iter.InBagPositionBegin();
        auto good_positions = GoodPositions(i);

        for (int i = 0; i < 3; i++) {
          REQUIRE(in_bag_iter.IsEnd() == false);
          uint32_t pos = in_bag_iter.Pop();
          REQUIRE(pos == good_positions[i]);
        }
        REQUIRE(in_bag_iter.IsEnd() == true);
      }
    }

    SECTION("Skip multiple times with large strides") {
      for (int i = 0; i < n_postings; i+=100) {
        pos_iter.SkipTo(i);

        InBagPositionIterator in_bag_iter = pos_iter.InBagPositionBegin();
        auto good_positions = GoodPositions(i);

        for (int i = 0; i < 3; i++) {
          REQUIRE(in_bag_iter.IsEnd() == false);
          uint32_t pos = in_bag_iter.Pop();
          REQUIRE(pos == good_positions[i]);
        }
        REQUIRE(in_bag_iter.IsEnd() == true);
      }
    }


  }
}

TEST_CASE( "Offset Bag iterator", "[qqflash][offset]" ) {
  // Build term frequency iterator
  std::vector<uint32_t> vec;
  int n_postings = 300;
  for (uint32_t i = 0; i < n_postings; i++) {
    vec.push_back(3);
  }

  std::string path = "/tmp/tmp.tf";
  FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
  SkipList tf_skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

  // Open the file
  utils::FileMap file_map(path);

  // Read data by TermFreqIterator
  TermFreqIterator tf_iter((const uint8_t *)file_map.Addr(), &tf_skip_list);

  SECTION("Check if the TF iterator works") {
    for (uint32_t i = 0; i < n_postings; i++) {
      tf_iter.SkipTo(i);
      REQUIRE(tf_iter.Value() == 3);
    }
  }

  SECTION("Offset iterator") {
    std::vector<uint32_t> offsets;
    for (uint32_t i = 0; i < n_postings * 3 * 2; i++) {
      offsets.push_back(i);
    }

    std::string path = "/tmp/tmp.off";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(offsets, path, false);
    std::vector<off_t> blob_offs = file_offsets.BlobOffsets();
    int n_intervals = (n_postings + PACK_SIZE - 1) / PACK_SIZE;
    
    std::vector<off_t> blob_offs_of_offset_bags; // for skip postings only
    std::vector<int> in_blob_indexes;
    for (int i = 0; i < n_intervals; i++) {
      blob_offs_of_offset_bags.push_back(blob_offs[i*6]);
      in_blob_indexes.push_back(0);
    }

    SkipList skip_list = CreateSkipListForOffset(
        blob_offs_of_offset_bags, in_blob_indexes);

    // Open the file
    utils::FileMap file_map(path);

    // Initialize pos iterator
    OffsetPostingBagIterator iter((const uint8_t*)file_map.Addr(), 
        skip_list, &tf_iter);

    SECTION("Very simple") {
      int posting_bag = 0;
      iter.SkipTo(posting_bag);
      int tf = iter.TermFreq();
      REQUIRE(tf == 3);

      auto good_offsets = GoodOffsets(posting_bag);

      for (int i = 0; i < tf * 2; i++) {
        uint32_t off = iter.Pop();
        REQUIRE(off == good_offsets[i]);
      }
    }

    SECTION("Very simple 2") {
      int posting_bag = 1;
      iter.SkipTo(posting_bag);
      int tf = iter.TermFreq();
      REQUIRE(tf == 3);

      auto good_offsets = GoodOffsets(posting_bag);

      for (int i = 0; i < tf * 2; i++) {
        uint32_t off = iter.Pop();
        REQUIRE(off == good_offsets[i]);
      }
    }

    SECTION("To the end") {
      int posting_bag = n_postings - 1;
      iter.SkipTo(posting_bag);
      int tf = iter.TermFreq();
      REQUIRE(tf == 3);

      auto good_offsets = GoodOffsets(posting_bag);

      for (int i = 0; i < tf * 2; i++) {
        uint32_t off = iter.Pop();
        REQUIRE(off == good_offsets[i]);
      }
    }

    SECTION("To the middle") {
      int posting_bag = n_postings / 2;
      iter.SkipTo(posting_bag);
      int tf = iter.TermFreq();
      REQUIRE(tf == 3);

      auto good_offsets = GoodOffsets(posting_bag);

      for (int i = 0; i < tf * 2; i++) {
        uint32_t off = iter.Pop();
        REQUIRE(off == good_offsets[i]);
      }
    }
  }
}



