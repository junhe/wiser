#include "catch.hpp"

#include "flash_engine_dumper.h"

VarintBuffer CreateVarintBuffer(std::vector<int> vec) {
  VarintBuffer buf;
  for (auto &val : vec) {
    buf.Append(val);
  }

  return buf;
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


TEST_CASE( "Offset Term Entry", "[qqflash][termentry]" ) {
  // There are deltas. The actual offsets are
  // 1, 4    12, 21
  VarintBuffer buf = CreateVarintBuffer({1, 3, 8, 9}); 
  CompressedPairIterator it(buf.DataPointer(), 0, buf.End());

  OffsetTermEntry entry(it);  
  REQUIRE(entry.Values() == std::vector<uint32_t>{1, 3, 8, 9});
}


TEST_CASE( "General term entry", "[qqflash]" ) {
  SECTION("Simple") {
    GeneralTermEntry entry;  
    entry.AddGroup({7});

    REQUIRE(entry.Values() == std::vector<uint32_t>{7});
    REQUIRE(entry.PostingSizes() == std::vector<int>{1});

    PostingLocations table = entry.LocationTable();

    REQUIRE(table.NumRows() == 1);
    REQUIRE(table[0].packed_block_idx == 0);
    REQUIRE(table[0].in_block_idx == 0);
  }

  SECTION("Multiple postings") {
    GeneralTermEntry entry;  
    entry.AddGroup({7});
    entry.AddGroup({9, 10});
    entry.AddGroup({11, 18});

    REQUIRE(entry.Values() == std::vector<uint32_t>{7, 9, 10, 11, 18});
    REQUIRE(entry.PostingSizes() == std::vector<int>{1, 2, 2});

    PostingLocations table = entry.LocationTable();

    REQUIRE(table.NumRows() == 3);
    REQUIRE(table[0].packed_block_idx == 0);
    REQUIRE(table[0].in_block_idx == 0);
    REQUIRE(table[1].packed_block_idx == 0);
    REQUIRE(table[1].in_block_idx == 1);
    REQUIRE(table[2].packed_block_idx == 0);
    REQUIRE(table[2].in_block_idx == 3);
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
    entry.AddGroup(vec);
    REQUIRE(entry.Values() == vec);

    TermEntryContainer container = entry.GetContainer(true);
    REQUIRE(container.PackWriters().size() == 1);
    REQUIRE(container.VInts().Size() == (200 - PackedIntsWriter::PACK_SIZE));

    SECTION("Dump it and read it") {
      // Dump it
      FileDumper dumper("/tmp/tmp.pos.dumper");
      PackFileOffsets file_offs = dumper.Dump(container);
      
      REQUIRE(file_offs.PackOffSize() == 1); 
      REQUIRE(file_offs.VIntsSize() == 1); 
      dumper.Flush();
      dumper.Close();

      // Read it
      int fd;
      char *addr;
      size_t file_length;
      utils::MapFile("/tmp/tmp.pos.dumper", &addr, &fd, &file_length);

      PackedIntsReader reader((uint8_t *)addr + file_offs.PackOffs()[0]);
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        REQUIRE(reader.Get(i) == deltas[i]);
      }

      utils::UnmapFile(addr, fd, file_length);
    }
  }
}


TEST_CASE( "PackFileOffsets", "[qqflash]" ) {
  PackFileOffsets offs({1, 10, 100}, {1000});
  REQUIRE(offs.FileOffset(0) == 1);
  REQUIRE(offs.FileOffset(1) == 10);
  REQUIRE(offs.FileOffset(2) == 100);
  REQUIRE(offs.FileOffset(3) == 1000);
}

TEST_CASE( "SkipPostingLocations", "[qqflash]" ) {
  PostingLocations posting_locations; 

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
  PackFileOffsets file_offs(pack_offs, vint_offs);

  SkipPostingLocations skip_locations(posting_locations, file_offs);

  REQUIRE(skip_locations.Size() == 3);
  REQUIRE(skip_locations[0].block_file_offset == 128 / 23);
  REQUIRE(skip_locations[1].block_file_offset == (128 * 2) / 23);
  REQUIRE(skip_locations[2].block_file_offset == (128 * 3) / 23);

}


TEST_CASE( "Dumping Engine", "[qqflash]" ) {
  FlashEngineDumper engine("/tmp");
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");

  SECTION("Check loading") {
    REQUIRE(engine.TermCount() > 0);
  }

  SECTION("Dumping inverted index") {
    engine.DumpInvertedIndex("/tmp/");
  }
}








