#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "utils.h"


TEST_CASE( "VInts writing and reading", "[qqflash][vints]" ) {
  VIntsWriter writer;

  SECTION("Empty") {
    std::string buf = writer.Serialize();

    VIntsIterator iter((const uint8_t *)buf.data());
    REQUIRE(iter.IsEnd() == true);
    REQUIRE(iter.SerializationSize() == 2);
  }

  SECTION("One number") {
    writer.Append(5);
    std::string buf = writer.Serialize();

    VIntsIterator iter((const uint8_t *) buf.data());
    REQUIRE(iter.SerializationSize() == 3);
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
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), &skip_list);
    
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
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), &skip_list);

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
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), &skip_list);

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
    TermFreqIterator iter((const uint8_t *)file_map.Addr(), &skip_list);

    SECTION("Skip one by one") {
      for (uint32_t i = 0; i < 100000; i++) {
        iter.SkipTo(i);
        REQUIRE(iter.Value() == i * 13);
      }

      file_map.Close();
    }
  }
}


TEST_CASE( "Doc id iterator", "[qqflash][docid]" ) {
  SECTION("Simple") {
    std::vector<uint32_t> doc_ids;
    int num_docids = 200;
    for (uint32_t i = 0; i < num_docids; i++) {
      doc_ids.push_back(i);
    }

    std::string path = "/tmp/tmp.docid";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(doc_ids, path, true);

    SkipList skip_list = CreateSkipListForDodId(
        GetSkipPostingPreDocIds(doc_ids), file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    DocIdIterator iter((const uint8_t *)file_map.Addr(), &skip_list, num_docids);

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

    SECTION("SkipForward() Simple") {
      REQUIRE(iter.Value() == 0);
      REQUIRE(iter.PostingIndex() == 0);

      iter.SkipForward(10);
      REQUIRE(iter.Value() == 10);
      REQUIRE(iter.PostingIndex() == 10);
    }

    SECTION("SkipForward() backwards ") {
      REQUIRE(iter.Value() == 0);

      iter.SkipForward(10);
      REQUIRE(iter.Value() == 10);

      iter.SkipForward(8);
      REQUIRE(iter.Value() == 10);
      REQUIRE(iter.PostingIndex() == 10);
    }

    SECTION("SkipForward() into vints ") {
      REQUIRE(iter.Value() == 0);

      iter.SkipForward(127); // end of pack
      REQUIRE(iter.Value() == 127);
      REQUIRE(iter.PostingIndex() == 127);

      iter.SkipForward(128); // start of vints
      REQUIRE(iter.Value() == 128);
      REQUIRE(iter.PostingIndex() == 128);

      iter.SkipForward(150); // start of vints
      REQUIRE(iter.Value() == 150);
      REQUIRE(iter.PostingIndex() == 150);
    }

    SECTION("SkipForward() beyond the end") {
      REQUIRE(iter.Value() == 0);

      iter.SkipForward(199); // start of vints
      REQUIRE(iter.Value() == 199);
      REQUIRE(iter.IsEnd() == false);

      iter.SkipForward(10000); // start of vints
      REQUIRE(iter.IsEnd() == true);
    }
  }

  SECTION("Exact one pack") {
    std::vector<uint32_t> doc_ids;
    int num_docids = 128;
    for (uint32_t i = 0; i < num_docids; i++) {
      doc_ids.push_back(i);
    }

    std::string path = "/tmp/tmp.docid";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(doc_ids, path, true);

    SkipList skip_list = CreateSkipListForDodId(
        GetSkipPostingPreDocIds(doc_ids), file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    DocIdIterator iter((const uint8_t *)file_map.Addr(), &skip_list, num_docids);

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

    SECTION("SkipForward() Simple") {
      REQUIRE(iter.Value() == 0);
      REQUIRE(iter.PostingIndex() == 0);

      iter.SkipForward(10);
      REQUIRE(iter.Value() == 10);
      REQUIRE(iter.PostingIndex() == 10);
    }

    SECTION("SkipForward() backwards ") {
      REQUIRE(iter.Value() == 0);

      iter.SkipForward(10);
      REQUIRE(iter.Value() == 10);

      iter.SkipForward(8);
      REQUIRE(iter.Value() == 10);
      REQUIRE(iter.PostingIndex() == 10);
    }

    SECTION("SkipForward() beyond the end") {
      REQUIRE(iter.Value() == 0);

      iter.SkipForward(199);
      // pointing to the end to indicate that we cannot find it
      REQUIRE(iter.IsEnd() == true); 
    }
  }

  SECTION("Large number of sequntial numbers") {
    std::vector<uint32_t> doc_ids;
    int num_docids = PACK_SIZE * 10 + 20;
    for (uint32_t i = 0; i < num_docids; i++) {
      doc_ids.push_back(i);
    }

    std::string path = "/tmp/tmp.docid";
    FileOffsetsOfBlobs file_offsets = DumpCozyBox(doc_ids, path, true);

    SkipList skip_list = CreateSkipListForDodId(
        GetSkipPostingPreDocIds(doc_ids), file_offsets.BlobOffsets());

    // Open the file
    utils::FileMap file_map(path);

    // Read data by TermFreqIterator
    DocIdIterator iter((const uint8_t *)file_map.Addr(), &skip_list, num_docids);

    SECTION("Numbers can be retrieved") {
      for (uint32_t i = 0; i < num_docids; i++) {
        REQUIRE(iter.Value() == i);
        REQUIRE(iter.PostingIndex() == i);
        REQUIRE(iter.IsEnd() == false);
        iter.Advance();
      }
      REQUIRE(iter.IsEnd() == true);

      file_map.Close();
    }

    SECTION("Go beyond the end") {
      iter.SkipForward(num_docids + 10);
      REQUIRE(iter.IsEnd() == true);
    }

    SECTION("To the last number") {
      iter.SkipForward(num_docids -1);
      REQUIRE(iter.Value() == num_docids -1);
      REQUIRE(iter.PostingIndex() == num_docids -1);
      REQUIRE(iter.IsEnd() == false);
    }
  }
}







