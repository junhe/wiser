#include "catch.hpp"

#include "vacuum_engine.h"
#include "flash_engine_dumper.h"


VarintBuffer CreateVarintBuffer(std::vector<int> vec) {
  VarintBuffer buf;
  for (auto &val : vec) {
    buf.Append(val);
  }

  return buf;
}

PostingBagBlobIndexes CreatePostingPackIndexes(
    std::vector<int> block_indexes, std::vector<int> in_block_indexes) {
  PostingBagBlobIndexes indexes;
  for (int i = 0; i < block_indexes.size(); i++) {
    indexes.AddRow(block_indexes[i], in_block_indexes[i]);
  }

  return indexes;
}

FileOffsetOfSkipPostingBags CreateFileOffsetOfSkipPostingBags(
    std::vector<int> block_indexes,
    std::vector<int> in_block_indexes,
    std::vector<off_t> pack_offs, 
    std::vector<off_t> vint_offs
    ) {
  PostingBagBlobIndexes pack_indexes = CreatePostingPackIndexes(
      block_indexes, in_block_indexes);
  
  FileOffsetsOfBlobs file_offs(pack_offs, vint_offs);
  return FileOffsetOfSkipPostingBags(pack_indexes, file_offs);
}


TEST_CASE( "Test File Dumper", "[qqflash]" ) {
  SECTION("initialize and destruct") {
    FileDumper dumper("/tmp/tmp.pos.dumper");
    dumper.Close();
  }
  
  SECTION("Get the current position") {
    FileDumper dumper("/tmp/tmp.pos.dumper");
    REQUIRE(dumper.CurrentOffset() == 0);
    dumper.Close();
  }
}


TEST_CASE( "Write and Read works", "[qqflash][utils]" ) {
  int fd = open("/tmp/tmp.writer.test", O_CREAT|O_RDWR|O_TRUNC, 00666);
  const int BUFSIZE = 10000;
  REQUIRE(fd != -1);

  // Write
  char *buf = (char *) malloc(BUFSIZE);
  memset(buf, 'a', BUFSIZE);

  REQUIRE(utils::Write(fd, buf, BUFSIZE) == BUFSIZE);

  // Read
  char *buf2 = (char *) malloc(BUFSIZE);
  lseek(fd, 0, SEEK_SET);
  REQUIRE(utils::Read(fd, buf2, BUFSIZE) == BUFSIZE);

  REQUIRE(memcmp(buf, buf2, BUFSIZE) == 0);

  close(fd);
}


TEST_CASE( "General term entry", "[qqflash]" ) {
  SECTION("Simple") {
    GeneralTermEntry entry;  
    entry.AddPostingBag({7});

    REQUIRE(entry.Values() == std::vector<uint32_t>{7});
    REQUIRE(entry.PostingBagSizes() == std::vector<int>{1});

    PostingBagBlobIndexes table = entry.GetPostingBagIndexes();

    REQUIRE(table.NumRows() == 1);
    REQUIRE(table[0].blob_index == 0);
    REQUIRE(table[0].in_blob_idx == 0);
  }

  SECTION("Multiple postings") {
    GeneralTermEntry entry;  
    entry.AddPostingBag({7});
    entry.AddPostingBag({9, 10});
    entry.AddPostingBag({11, 18});

    REQUIRE(entry.Values() == std::vector<uint32_t>{7, 9, 10, 11, 18});
    REQUIRE(entry.PostingBagSizes() == std::vector<int>{1, 2, 2});

    PostingBagBlobIndexes table = entry.GetPostingBagIndexes();

    REQUIRE(table.NumRows() == 3);
    REQUIRE(table[0].blob_index == 0);
    REQUIRE(table[0].in_blob_idx == 0);
    REQUIRE(table[1].blob_index == 0);
    REQUIRE(table[1].in_blob_idx == 1);
    REQUIRE(table[2].blob_index == 0);
    REQUIRE(table[2].in_blob_idx == 3);
  }

  SECTION("More than one packed block") {
    std::vector<uint32_t> vec;
    std::vector<uint32_t> deltas;
    int prev = 0;
    for (int i = 0; i < 200; i++) {
      int delta = i % 7;
      deltas.push_back(delta);

      int num = prev + delta;
      vec.push_back(num); 
      prev = num;
    }

    GeneralTermEntry entry;
    entry.AddPostingBag(vec);
    REQUIRE(entry.Values() == vec);

    CozyBoxWriter writer = entry.GetCozyBoxWriter(true);
    REQUIRE(writer.PackWriters().size() == 1);
    REQUIRE(writer.VInts().IntsSize() == (200 - PackedIntsWriter::PACK_SIZE));

    SECTION("Dump it and read it") {
      // Dump it
      FileDumper dumper("/tmp/tmp.pos.dumper");
      FakeFileDumper fake_dumper("/tmp/tmp.fake.dumper");

      FileOffsetsOfBlobs file_offs = dumper.Dump(writer);
      FileOffsetsOfBlobs file_offs2 = fake_dumper.Dump(writer);
      REQUIRE(file_offs == file_offs2);
      
      REQUIRE(file_offs.PackOffSize() == 1); 
      REQUIRE(file_offs.VIntsSize() == 1); 
      dumper.Flush();
      dumper.Close();

      // Read it
      int fd;
      char *addr;
      size_t file_length;
      utils::MapFile("/tmp/tmp.pos.dumper", &addr, &fd, &file_length);

      LittlePackedIntsReader reader((uint8_t *)addr + file_offs.PackOffs()[0]);
      reader.DecodeToCache();
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        REQUIRE(reader.Get(i) == deltas[i]);
      }

      utils::UnmapFile(addr, fd, file_length);
    }
  }
}


TEST_CASE( "FileOffsetsOfBlobs", "[qqflash]" ) {
  FileOffsetsOfBlobs offs({1, 10, 100}, {1000});
  REQUIRE(offs.FileOffset(0) == 1);
  REQUIRE(offs.FileOffset(1) == 10);
  REQUIRE(offs.FileOffset(2) == 100);
  REQUIRE(offs.FileOffset(3) == 1000);
}


TEST_CASE( "FileOffsetOfSkipPostingBags", "[qqflash]" ) {
  PostingBagBlobIndexes posting_locations; 

  int n_postings = SKIP_INTERVAL * 3 + 10;
  for (int i = 0; i < n_postings; i++) {
    posting_locations.AddRow(i / 23, i);
  }

  std::vector<off_t> pack_offs;
  int n_packs = n_postings / 23;
  for (int i = 0; i < n_packs; i++) {
    pack_offs.push_back(i);
  }
  std::vector<off_t> vint_offs{1000};
  FileOffsetsOfBlobs file_offs(pack_offs, vint_offs);

  FileOffsetOfSkipPostingBags skip_locations(posting_locations, file_offs);

  REQUIRE(skip_locations.Size() == 4);
  REQUIRE(skip_locations[0].file_offset_of_blob == 0);
  REQUIRE(skip_locations[1].file_offset_of_blob == 128 / 23);
  REQUIRE(skip_locations[2].file_offset_of_blob == (128 * 2) / 23);
  REQUIRE(skip_locations[3].file_offset_of_blob == (128 * 3) / 23);

}


template <typename T=int>
std::vector<T> RepeatNums(std::vector<T> vec) {
  std::vector<T> ret;
  for (auto &x : vec) {
    for (int i = 0; i < SKIP_INTERVAL; i++) {
      ret.push_back(x);
    }
  }
  return ret;
}


TEST_CASE( "SkipListWriter", "[qqflash]" ) {
  SECTION("One skip entry") {
    auto doc_id_offs = CreateFileOffsetOfSkipPostingBags({0}, {1}, {10}, {20});     
    auto tf_offs = CreateFileOffsetOfSkipPostingBags({0}, {2}, {11}, {21});     
    auto pos_offs = CreateFileOffsetOfSkipPostingBags({0}, {3}, {12}, {22});     
    auto off_offs = CreateFileOffsetOfSkipPostingBags({0}, {4}, {13}, {23});     
    auto doc_ids = std::vector<uint32_t>{18};

    SkipListWriter writer(doc_id_offs, tf_offs, pos_offs, off_offs, doc_ids);

    SECTION("Loading to skiplist") {
      std::string data = writer.Serialize();

      SkipList skip_list;
      skip_list.Load((uint8_t *)data.data());

      REQUIRE(skip_list.NumEntries() == 1);
      REQUIRE(skip_list[0].previous_doc_id == 0);
      REQUIRE(skip_list[0].file_offset_of_docid_bag == 10);
      REQUIRE(skip_list[0].file_offset_of_tf_bag == 11);
      REQUIRE(skip_list[0].file_offset_of_pos_blob == 12);
      REQUIRE(skip_list[0].in_blob_index_of_pos_bag == 3);
      REQUIRE(skip_list[0].file_offset_of_offset_blob == 13);
    }

    SECTION("Term dict file dumping and loading") {
      std::string path = "/tmp/my.tim";
      TermDictFileDumper file_dumper(path);

      off_t entry1_off = file_dumper.DumpSkipList(8888, writer.Serialize());
      off_t entry2_off = file_dumper.DumpSkipList(9999, writer.Serialize());

      file_dumper.Close();

      utils::FileMap map;
      map.Open(path);
      char *addr = map.Addr();

      SECTION("First entry") {
        TermDictEntry entry;
        entry.Load(addr + entry1_off);

        REQUIRE(entry.DocFreq() == 8888);
        
        const SkipList &skip_list = entry.GetSkipList();

        REQUIRE(skip_list.NumEntries() == 1);
        REQUIRE(skip_list[0].previous_doc_id == 0);
        REQUIRE(skip_list[0].file_offset_of_docid_bag == 10);
        REQUIRE(skip_list[0].file_offset_of_tf_bag == 11);
        REQUIRE(skip_list[0].file_offset_of_pos_blob == 12);
        REQUIRE(skip_list[0].in_blob_index_of_pos_bag == 3);
        REQUIRE(skip_list[0].file_offset_of_offset_blob == 13);
      }

      SECTION("Second entry") {
        TermDictEntry entry;
        entry.Load(addr + entry2_off);

        REQUIRE(entry.DocFreq() == 9999);
        
        const SkipList &skip_list = entry.GetSkipList();

        REQUIRE(skip_list.NumEntries() == 1);
        REQUIRE(skip_list[0].previous_doc_id == 0);
        REQUIRE(skip_list[0].file_offset_of_docid_bag == 10);
        REQUIRE(skip_list[0].file_offset_of_tf_bag == 11);
        REQUIRE(skip_list[0].file_offset_of_pos_blob == 12);
        REQUIRE(skip_list[0].in_blob_index_of_pos_bag == 3);
        REQUIRE(skip_list[0].file_offset_of_offset_blob == 13);
      }

    }
  }

  SECTION("Two skip entries") {
    auto doc_id_offs = CreateFileOffsetOfSkipPostingBags(
        RepeatNums({0, 1}), RepeatNums({1, 2}), {10, 11}, {20});     
    auto tf_offs = CreateFileOffsetOfSkipPostingBags(
        RepeatNums({0, 1}), RepeatNums({3, 4}), {12, 13}, {21});     
    auto pos_offs = CreateFileOffsetOfSkipPostingBags(
        RepeatNums({0, 1}), RepeatNums({5, 6}), {14, 15}, {22});     
    auto off_offs = CreateFileOffsetOfSkipPostingBags(
        RepeatNums({0, 1}), RepeatNums({7, 8}), {16, 17}, {23});     
    auto doc_ids = RepeatNums(std::vector<uint32_t>{18, 2999});

    SkipListWriter writer(doc_id_offs, tf_offs, pos_offs, off_offs, doc_ids);

    std::string data = writer.Serialize();

    SkipList skip_list;
    skip_list.Load((uint8_t *)data.data());

    REQUIRE(skip_list.NumEntries() == 2);

    REQUIRE(skip_list[0].previous_doc_id == 0);
    REQUIRE(skip_list[0].file_offset_of_docid_bag == 10);
    REQUIRE(skip_list[0].file_offset_of_tf_bag == 12);
    REQUIRE(skip_list[0].file_offset_of_pos_blob == 14);
    REQUIRE(skip_list[0].in_blob_index_of_pos_bag == 5);
    REQUIRE(skip_list[0].file_offset_of_offset_blob == 16);

    REQUIRE(skip_list[1].previous_doc_id == 18);
    REQUIRE(skip_list[1].file_offset_of_docid_bag == 11);
    REQUIRE(skip_list[1].file_offset_of_tf_bag == 13);
    REQUIRE(skip_list[1].file_offset_of_pos_blob == 15);
    REQUIRE(skip_list[1].in_blob_index_of_pos_bag == 6);
    REQUIRE(skip_list[1].file_offset_of_offset_blob == 17);
  }
}


TEST_CASE( "Term Dict File Dumper", "[qqflash]" ) {
  TermDictFileDumper dumper("/tmp/my.tim"); 
  off_t entry1 = dumper.DumpSkipList(10, "xyz");
  REQUIRE(entry1 == 0);

  off_t entry2 = dumper.DumpFullPostingList(7, "abcd");
  REQUIRE(entry2 == 6);

  dumper.Close();

  // Read it
  int fd;
  char *addr;
  size_t file_length;
  utils::MapFile("/tmp/my.tim", &addr, &fd, &file_length);

  REQUIRE(file_length == 13);

  {
  VarintIterator it(addr, 0, 3);
  REQUIRE(it.Pop() == 0);
  REQUIRE(it.Pop() == 10);
  REQUIRE(it.Pop() == 3);
  }

  REQUIRE(memcmp(addr + 3, "xyz", 3) == 0);

  {
  VarintIterator it(addr + 6, 0, 3);
  REQUIRE(it.Pop() == 1);
  REQUIRE(it.Pop() == 7);
  REQUIRE(it.Pop() == 4);
  }

  REQUIRE(memcmp(addr + 6 + 3, "abcd", 4) == 0);

  utils::UnmapFile(addr, fd, file_length);
}


TEST_CASE( "Term Index works", "[qqflash]" ) {
  std::string path = "/tmp/my.tip";

  // Dump
  TermIndexDumper dumper(path);

  dumper.DumpEntry("hello", 0);
  dumper.DumpEntry("wisc", 10032);

  dumper.Close();

  //Read
  TermIndex index;
  index.Load(path);

  {
  auto it = index.Find("hello");
  REQUIRE(it.IsEmpty() == false);
  REQUIRE(it.Key() == "hello");
  REQUIRE(it.GetPostingListOffset() == 0);
  REQUIRE(it.GetNumPagesInPrefetchZone() == 0);
  }

  {
  auto it = index.Find("wisc");
  REQUIRE(it.IsEmpty() == false);
  REQUIRE(it.Key() == "wisc");
  REQUIRE(it.GetPostingListOffset() == 10032);
  REQUIRE(it.GetNumPagesInPrefetchZone() == 0);
  }

  {
  auto it = index.Find("notexist");
  REQUIRE(it.IsEmpty() == true);
  }
}

TEST_CASE( "Term Trie Index works", "[qqflash]" ) {
  std::string path = "/tmp/my.tip";

  // Dump
  TermIndexDumper dumper(path);

  dumper.DumpEntry("hello", 0);
  dumper.DumpEntry("wisc", 10032);

  dumper.Close();

  //Read
  TermTrieIndex index;
  index.Load(path);

  {
  auto it = index.Find("hello");
  REQUIRE(it.IsEmpty() == false);
  REQUIRE(it.Key() == "hello");
  REQUIRE(it.GetPostingListOffset() == 0);
  REQUIRE(it.GetNumPagesInPrefetchZone() == 0);
  }

  {
  auto it = index.Find("wisc");
  REQUIRE(it.IsEmpty() == false);
  REQUIRE(it.Key() == "wisc");
  REQUIRE(it.GetPostingListOffset() == 10032);
  REQUIRE(it.GetNumPagesInPrefetchZone() == 0);
  }

  {
  auto it = index.Find("notexist");
  REQUIRE(it.IsEmpty() == true);
  }
}




TEST_CASE( "QQFlash Compressed Doc Store", "[qqflash]" ) {
  SECTION("Index Then Search") {
    // index
    FlashDocStoreDumper store;
    int doc_id = 0;
    std::string doc = "it is a doc";
    store.Add(doc_id, doc);
    REQUIRE(store.Get(doc_id) == doc);

    store.Add(1, "doc1");
    REQUIRE(store.Size() == 2);
    REQUIRE(store.Has(doc_id) == true);

    store.Dump("/tmp/my.fdx", "/tmp/my.fdt");
    store.Clear();

    // search
    FlashDocStore search_store;
    search_store.Load("/tmp/my.fdx", "/tmp/my.fdt");

    REQUIRE(search_store.Size() == 2);
    REQUIRE(search_store.Has(0));
    REQUIRE(search_store.Has(1));
    REQUIRE(search_store.Has(2) == false);

    REQUIRE(search_store.Get(0) == doc);
    REQUIRE(search_store.Get(1) == "doc1");
  }
}


TEST_CASE( "General file dumper", "[qqflash]" ) {
  GeneralFileDumper dumper("/tmp/dumper.test");
  REQUIRE(dumper.CurrentOffset() == 0);
  REQUIRE(dumper.End() == 0);

  dumper.Dump("12345");

  REQUIRE(dumper.CurrentOffset() == 5);
  REQUIRE(dumper.End() == 5);

  dumper.Seek(1);
  REQUIRE(dumper.CurrentOffset() == 1);

  dumper.SeekToEnd();
  REQUIRE(dumper.CurrentOffset() == 5);
  REQUIRE(dumper.End() == 5);
}


