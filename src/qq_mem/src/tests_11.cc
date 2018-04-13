#include "catch.hpp"

#include "flash_iterators.h"
#include "utils.h"


TEST_CASE( "VInts writing and reading", "[qqflash][vints]" ) {
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
FileOffsetsOfBlobs DumpCozyBox(std::vector<uint32_t> vec, 
    const std::string path, bool do_delta) {
  GeneralTermEntry entry;
  for (auto x : vec) {
    entry.AddPostingBag({x});
  }
  CozyBoxWriter writer = entry.GetCozyBoxWriter(do_delta);

  FileDumper file_dumper(path);
  FileOffsetsOfBlobs file_offs = file_dumper.Dump(writer);
  file_dumper.Close();

  return file_offs;
}

SkipList CreateSkipList(const std::string type, std::vector<off_t> offsets_of_bags) {
  SkipList skip_list; 
  
  for (int i = 0; i < offsets_of_bags.size(); i++) {
    if (type == "TF") {
      skip_list.AddEntry(10000 + i, 0, offsets_of_bags[i], 0, 0, 0); 
    } else {
      LOG(FATAL) << "Type " << type << " not supported yet";
    }
  }

  return skip_list;
}


SkipList CreateSkipListForDodId(
    std::vector<uint32_t> skip_doc_ids, std::vector<off_t> offsets_of_bags) {
  SkipList skip_list; 
  
  for (int i = 0; i < offsets_of_bags.size(); i++) {
    skip_list.AddEntry(skip_doc_ids[i], offsets_of_bags[i], 0, 0, 0, 0); 
  }

  return skip_list;
}



TEST_CASE( "TermFreqIterator", "[qqflash][tf_iter]" ) {
  SECTION("Simple") {
    std::vector<uint32_t> vec;
    for (uint32_t i = 0; i < 300; i++) {
      vec.push_back(i);
    }

    std::string path = "/tmp/tmp.tf";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
    SkipList skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

    REQUIRE(skip_list.NumEntries() == 3);

    // Just a simple sanity check
    REQUIRE(skip_list[0].file_offset_of_tf_bag == 0);
    REQUIRE(skip_list[1].file_offset_of_tf_bag > 0);
    REQUIRE(skip_list[2].file_offset_of_tf_bag > 0);
    REQUIRE(skip_list[2].file_offset_of_tf_bag > skip_list[1].file_offset_of_tf_bag);


    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), skip_list);
    
    for (uint32_t i = 0; i < 300; i++) {
      iter.SkipTo(i);
      REQUIRE(iter.Value() == i);
    }

    file_map.Close();
  }

  SECTION("large numbers") {
    std::vector<uint32_t> vec;
    for (uint32_t i = 0; i < 300; i++) {
      vec.push_back(i * 131);
    }

    std::string path = "/tmp/tmp.tf2";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
    SkipList skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), skip_list);

    SECTION("Skip one by one") {
      for (uint32_t i = 0; i < 300; i++) {
        iter.SkipTo(i);
        REQUIRE(iter.Value() == i * 131);
      }

      file_map.Close();
    }

    SECTION("Skip with strides") {
      for (uint32_t i = 0; i < 300; i += 20) {
        iter.SkipTo(i);
        REQUIRE(iter.Value() == i * 131);
      }

      file_map.Close();
    }
  }

  SECTION("small number ints") {
    std::vector<uint32_t> vec;
    for (uint32_t i = 0; i < 1; i++) {
      vec.push_back(i * 131);
    }

    std::string path = "/tmp/tmp.tf2";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
    SkipList skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), skip_list);

    SECTION("Skip one by one") {
      for (uint32_t i = 0; i < 1; i++) {
        iter.SkipTo(i);
        REQUIRE(iter.Value() == i * 131);
      }

      file_map.Close();
    }
  }

  SECTION("Large number of ints") {
    std::vector<uint32_t> vec;
    for (uint32_t i = 0; i < 100000; i++) {
      vec.push_back(i * 13);
    }

    std::string path = "/tmp/tmp.tf2";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(vec, path, false);
    SkipList skip_list = CreateSkipList("TF", file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), skip_list);

    SECTION("Skip one by one") {
      for (uint32_t i = 0; i < 100000; i++) {
        iter.SkipTo(i);
        REQUIRE(iter.Value() == i * 13);
      }

      file_map.Close();
    }
  }
}


TEST_CASE( "Doc id iterator", "[qqflash]" ) {
  SECTION("Simple") {
    std::vector<uint32_t> doc_ids;
    for (uint32_t i = 0; i < 10; i++) {
      doc_ids.push_back(i * 13);
    }

    std::string path = "/tmp/tmp.docid";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(doc_ids, path, true);

    SkipList skip_list = CreateSkipListForDodId(
        GetSkipPostingDocIds(doc_ids), file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    DocIdIterator iter((const uint8_t *)file_map.Addr(), skip_list);

    SECTION("Skip one by one") {
      for (uint32_t i = 0; i < 10; i++) {
        iter.SkipTo(i);
        // REQUIRE(iter.Value() == i * 13);
      }

      file_map.Close();
    }
  }
}






