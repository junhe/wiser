#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "utils.h"


TEST_CASE( "CozyBoxIterator", "[qqflash][cozy]" ) {
  SECTION("Only some vints") {
    std::vector<uint32_t> vec;
    uint32_t cnt = 8;
    for (uint32_t i = 0; i < cnt; i++) {
      vec.push_back(i);
    }

    std::string path = "/tmp/tmp.tf";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);

    // Open the file
    utils::FileMap file_map;
    file_map.Open(path);

    SECTION("Advance() to the end") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());

      iter.GoToCozyEntry(0, 0);
      REQUIRE(iter.Value() == 0);
      
      for (uint32_t i = 1; i < cnt; i++) {
        std::cout << "i: " << i << std::endl;
        iter.Advance();
        REQUIRE(iter.Value() == i);
      }
    }

    SECTION("AdvanceBy()") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(0, 0);

      REQUIRE(iter.Value() == 0);

      iter.AdvanceBy(1);
      REQUIRE(iter.Value() == 1);

      iter.AdvanceBy(1);
      REQUIRE(iter.Value() == 2);

      iter.AdvanceBy(2);
      REQUIRE(iter.Value() == 4);
    }
  }



  SECTION("2 packs, some vints") {
    std::vector<uint32_t> vec;
    uint32_t cnt = 300;
    for (uint32_t i = 0; i < cnt; i++) {
      vec.push_back(i);
    }

    std::string path = "/tmp/tmp.tf";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);

    // Open the file
    utils::FileMap file_map;
    file_map.Open(path);


    SECTION("Simple") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());

      iter.GoToCozyEntry(0, 0);
      REQUIRE(iter.Value() == 0);
      REQUIRE(iter.CurBlobOffset() == 0);
    }

    SECTION("Advance() to the end") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(0, 0);
      REQUIRE(iter.Value() == 0);
      
      for (uint32_t i = 1; i < cnt; i++) {
        iter.Advance();
        REQUIRE(iter.Value() == i);
      }
    }

    SECTION("Start from the last part of pack") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(0, PACK_SIZE - 10);
      REQUIRE(iter.Value() == PACK_SIZE - 10);
      
      for (uint32_t i = PACK_SIZE - 10 + 1; i < cnt; i++) {
        iter.Advance();
        REQUIRE(iter.Value() == i);
      }
    }

    SECTION("Start from the second packints") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(file_offsets.PackOffs()[1], 0);
      REQUIRE(iter.Value() == 128);
      
      for (uint32_t i = PACK_SIZE + 1; i < cnt; i++) {
        iter.Advance();
        REQUIRE(iter.Value() == i);
      }
    }

    SECTION("Start from the second packints, 2") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(file_offsets.PackOffs()[1], 2);
      REQUIRE(iter.Value() == 130);
      
      for (uint32_t i = PACK_SIZE + 2 + 1; i < cnt; i++) {
        iter.Advance();
        REQUIRE(iter.Value() == i);
      }
    }

    SECTION("Start from the vints") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(file_offsets.VIntsOffs()[0], 0);
      REQUIRE(iter.Value() == 256);
      
      for (uint32_t i = PACK_SIZE * 2 + 1; i < cnt; i++) {
        iter.Advance();
        REQUIRE(iter.Value() == i);
      }
    }

    SECTION("AdvanceBy()") {
      CozyBoxIterator iter((const uint8_t *)file_map.Addr());
      iter.GoToCozyEntry(0, 0);
      REQUIRE(iter.Value() == 0);

      iter.AdvanceBy(2);
      REQUIRE(iter.Value() == 2);

      iter.AdvanceBy(124);
      REQUIRE(iter.Value() == 126);

      iter.AdvanceBy(2);
      REQUIRE(iter.Value() == 128);

      iter.AdvanceBy(128);
      REQUIRE(iter.Value() == 256);

      iter.AdvanceBy(1);
      REQUIRE(iter.Value() == 257);
    }
  }
}


std::vector<uint32_t> GoodPositions(uint32_t posting_bag) {
  std::vector<uint32_t> ret;
  uint32_t prev = 0;
  for (uint32_t delta = posting_bag * 3; delta < (posting_bag + 1) * 3; delta++) {
    uint32_t pos = prev + delta;
    ret.push_back(pos);
    prev = pos; 
  }

  return ret;
}

std::vector<uint32_t> GoodOffsets(uint32_t posting_bag) {
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
  uint32_t n_postings = 300;
  for (uint32_t i = 0; i < n_postings; i++) {
    vec.push_back(3);
  }

  std::string path = "/tmp/tmp.tf";
  FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
  SkipList tf_skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

  // Open the file
  utils::FileMap file_map;
  file_map.Open(path);

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
    utils::FileMap file_map;
    file_map.Open(path);

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
      for (uint32_t i = 0; i < n_postings; i++) {
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
      for (uint32_t i = 0; i < n_postings; i+=7) {
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
      for (uint32_t i = 0; i < n_postings; i+=100) {
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


void CheckOffsets(LazyBoundedOffsetPairIterator in_bag_iter,
                  std::vector<uint32_t> good_offsets) 
{
    // tf == 3
    for (int i = 0; i < 3; i++) {
      OffsetPair pair;
      in_bag_iter.Pop(&pair);
      REQUIRE(std::get<0>(pair) == good_offsets[2*i]);
      REQUIRE(std::get<1>(pair) == good_offsets[2*i + 1]);
    }
    REQUIRE(in_bag_iter.IsEnd());
}


TEST_CASE( "Offset Bag iterator", "[qqflash][offset]" ) {
  // Build term frequency iterator
  std::vector<uint32_t> vec;
  uint32_t n_postings = 300;
  for (uint32_t i = 0; i < n_postings; i++) {
    vec.push_back(3);
  }

  std::string path = "/tmp/tmp.tf";
  FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
  SkipList tf_skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

  // Open the file
  utils::FileMap file_map;
  file_map.Open(path);

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
    utils::FileMap file_map;
    file_map.Open(path);

    // Initialize pos iterator
    OffsetPostingBagIterator iter((const uint8_t*)file_map.Addr(), 
        &skip_list, tf_iter);

    SECTION("Very simple") {
      int posting_bag = 0;
      iter.SkipTo(posting_bag);
      
      InBagOffsetPairIterator in_bag_iter = iter.InBagOffsetPairBegin();

      auto good_offsets = GoodOffsets(posting_bag);

      // tf == 3
      for (int i = 0; i < 3; i++) {
        OffsetPair pair;
        in_bag_iter.Pop(&pair);
        REQUIRE(std::get<0>(pair) == good_offsets[2*i]);
        REQUIRE(std::get<1>(pair) == good_offsets[2*i + 1]);
      }
    }

    SECTION("Very simple, LazyBoundedOffsetPairIterator") {
      int posting_bag = 0;
      REQUIRE(iter.TermFreq() == 3);
      LazyBoundedOffsetPairIterator in_bag_iter(posting_bag, iter);
      
      auto good_offsets = GoodOffsets(posting_bag);
      CheckOffsets(in_bag_iter, good_offsets);
    }

    SECTION("Very simple 2, lazy") {
      int posting_bag = 1;
      // tf = 3
      LazyBoundedOffsetPairIterator in_bag_iter(posting_bag, iter);
      
      auto good_offsets = GoodOffsets(posting_bag);
      CheckOffsets(in_bag_iter, good_offsets);
    }

    SECTION("Very simple 2") {
      int posting_bag = 1;
      iter.SkipTo(posting_bag);
      
      InBagOffsetPairIterator in_bag_iter = iter.InBagOffsetPairBegin();

      auto good_offsets = GoodOffsets(posting_bag);

      // tf == 3
      for (int i = 0; i < 3; i++) {
        OffsetPair pair;
        in_bag_iter.Pop(&pair);
        REQUIRE(std::get<0>(pair) == good_offsets[2*i]);
        REQUIRE(std::get<1>(pair) == good_offsets[2*i + 1]);
      }
    }

    SECTION("To the end") {
      int posting_bag = n_postings - 1;
      iter.SkipTo(posting_bag);

      InBagOffsetPairIterator in_bag_iter = iter.InBagOffsetPairBegin();

      auto good_offsets = GoodOffsets(posting_bag);

      // tf == 3
      for (int i = 0; i < 3; i++) {
        OffsetPair pair;
        in_bag_iter.Pop(&pair);
        REQUIRE(std::get<0>(pair) == good_offsets[2*i]);
        REQUIRE(std::get<1>(pair) == good_offsets[2*i + 1]);
      }
    }

    SECTION("To the end, lazy") {
      int posting_bag = n_postings - 1;

      // tf = 3
      LazyBoundedOffsetPairIterator in_bag_iter(posting_bag, iter);
      auto good_offsets = GoodOffsets(posting_bag);

      CheckOffsets(in_bag_iter, good_offsets);
    }

    SECTION("To the middle, lazy") {
      int posting_bag = n_postings / 2;

      // tf = 3
      LazyBoundedOffsetPairIterator in_bag_iter(posting_bag, iter);
      
      auto good_offsets = GoodOffsets(posting_bag);
      CheckOffsets(in_bag_iter, good_offsets);
    }

    SECTION("To the middle") {
      int posting_bag = n_postings / 2;
      iter.SkipTo(posting_bag);

      InBagOffsetPairIterator in_bag_iter = iter.InBagOffsetPairBegin();

      auto good_offsets = GoodOffsets(posting_bag);

      // tf == 3
      for (int i = 0; i < 3; i++) {
        OffsetPair pair;
        in_bag_iter.Pop(&pair);
        REQUIRE(std::get<0>(pair) == good_offsets[2*i]);
        REQUIRE(std::get<1>(pair) == good_offsets[2*i + 1]);
      }
    }

    SECTION("Iterate all posting bags, lazy") {
      for (uint32_t posting_bag = 0; posting_bag < n_postings; posting_bag++) {
        LazyBoundedOffsetPairIterator in_bag_iter(posting_bag, iter);
        auto good_offsets = GoodOffsets(posting_bag);
        CheckOffsets(in_bag_iter, good_offsets);
      }
    }

    SECTION("Iterate all posting bags, with strides, lazy") {
      for (uint32_t posting_bag = 0; posting_bag < n_postings; posting_bag += 7) {
        LazyBoundedOffsetPairIterator in_bag_iter(posting_bag, iter);
        auto good_offsets = GoodOffsets(posting_bag);
        CheckOffsets(in_bag_iter, good_offsets);
      }
    }
 
    SECTION("Iterate all posting bags") {
      for (uint32_t posting_bag = 0; posting_bag < n_postings; posting_bag++) {
        iter.SkipTo(posting_bag);

        InBagOffsetPairIterator in_bag_iter = iter.InBagOffsetPairBegin();

        auto good_offsets = GoodOffsets(posting_bag);

        // tf == 3
        for (int i = 0; i < 3; i++) {
          OffsetPair pair;
          in_bag_iter.Pop(&pair);
          REQUIRE(std::get<0>(pair) == good_offsets[2*i]);
          REQUIRE(std::get<1>(pair) == good_offsets[2*i + 1]);
        }
      }
    }

    SECTION("Iterate all posting bags, with strides") {
      for (uint32_t posting_bag = 0; posting_bag < n_postings; posting_bag += 7) {
        iter.SkipTo(posting_bag);

        InBagOffsetPairIterator in_bag_iter = iter.InBagOffsetPairBegin();

        auto good_offsets = GoodOffsets(posting_bag);

        // tf == 3
        for (int i = 0; i < 3; i++) {
          OffsetPair pair;
          in_bag_iter.Pop(&pair);
          REQUIRE(std::get<0>(pair) == good_offsets[2*i]);
          REQUIRE(std::get<1>(pair) == good_offsets[2*i + 1]);
        }
      }
    }
  }
}



