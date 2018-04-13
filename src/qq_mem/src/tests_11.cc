#include "catch.hpp"

#include "flash_iterators.h"


TEST_CASE( "VInts writing and reading", "[qqflash]" ) {
  VIntsWriter writer;

  SECTION("Empty") {
    std::string buf = writer.Serialize();

    VIntsIterator iter((const uint8_t *)buf.data());
    REQUIRE(iter.IsEnd() == true);
  }

  SECTION("One number") {
    writer.Append(5);
    std::string buf = writer.Serialize();

    VIntsIterator iter((const uint8_t *) buf.data());
    REQUIRE(iter.IsEnd() == false);

    REQUIRE(iter.Pop() == 5);
    REQUIRE(iter.IsEnd() == true);
  }

  SECTION("Three numbers") {
    writer.Append(1000);
    writer.Append(2);
    writer.Append(13377200);
    std::string buf = writer.Serialize();

    VIntsIterator iter((const uint8_t *) buf.data());

    SECTION("Pop all") {
      REQUIRE(iter.IsEnd() == false);

      REQUIRE(iter.Pop() == 1000);
      REQUIRE(iter.Pop() == 2);
      REQUIRE(iter.Pop() == 13377200);
      REQUIRE(iter.IsEnd() == true);
    }
    
    SECTION("SkipTo and Peek") {
      iter.SkipTo(0);
      REQUIRE(iter.Peek() == 1000);

      iter.SkipTo(2);
      REQUIRE(iter.Peek() == 13377200);
      REQUIRE(iter.IsEnd() == false);

      REQUIRE(iter.Pop() == 13377200);
      REQUIRE(iter.IsEnd() == true);
    }
  }

  SECTION("256 - 300") {
    for (int i = 256; i < 300; i++) {
      writer.Append(i);
    }

    std::string buf = writer.Serialize();

    SECTION("by popping") {
      VIntsIterator iter((const uint8_t *) buf.data());

      for (int i = 256; i < 300; i++) {
        REQUIRE(iter.Pop() == i);
      }
    }

    SECTION("by peeking") {
      VIntsIterator iter((const uint8_t *) buf.data());

      int index = 0;
      for (int i = 256; i < 300; i++) {
        iter.SkipTo(index);
        REQUIRE(iter.Peek() == i);
        index++;
      }
    }
  }
}


// Dump a cozy box, return FileOffsetsOfBlobs
FileOffsetsOfBlobs DumpCozyBox(std::vector<uint32_t> vec) {
  GeneralTermEntry entry;
  for (uint32_t i = 0; i < 300; i++) {
    entry.AddPostingBag({i});
  }
  CozyBoxWriter writer = entry.GetCozyBoxWriter(false);

  FileDumper file_dumper("/tmp/tmp.tf");
  FileOffsetsOfBlobs file_offs = file_dumper.Dump(writer);

  return file_offs;
}

SkipList CreateSkipList(const std::string type, std::vector<off_t> offsets_of_bags) {
  SkipList skip_list; 
  
  for (int i = 0; i < offsets_of_bags.size(); i++) {
    if (type == "TF") {
      std::cout << "ofset: " << offsets_of_bags[i] << std::endl;
      skip_list.AddEntry(10000 + i, 0, offsets_of_bags[i], 0, 0, 0); 
    } else {
      LOG(FATAL) << "Type " << type << " not supported yet";
    }
  }

  return skip_list;
}

TEST_CASE( "TermFreqIterator", "[qqflash]" ) {
  // Dump TF without delta to a file
  // Create skip list using the return of Dump()
  // Create TermFreqIterator

  std::vector<uint32_t> vec;
  for (uint32_t i = 0; i < 300; i++) {
    vec.push_back(i);
  }

  FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec);
  SkipList skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

  REQUIRE(skip_list.NumEntries() == 3);

  // Just a simple sanity check
  REQUIRE(skip_list[0].file_offset_of_tf_bag == 0);
  REQUIRE(skip_list[1].file_offset_of_tf_bag > 0);
  REQUIRE(skip_list[2].file_offset_of_tf_bag > 0);
  REQUIRE(skip_list[2].file_offset_of_tf_bag > skip_list[1].file_offset_of_tf_bag);


}





