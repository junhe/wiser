#include "catch.hpp"

#include "flash_engine_dumper.h"

VarintBuffer CreateVarintBuffer(std::vector<int> vec) {
  VarintBuffer buf;
  for (auto &val : vec) {
    buf.Append(val);
  }

  return buf;
}


TEST_CASE( "Test PositionTermEntry", "[qqflash]" ) {
  SECTION("Simple case") {
    VarintBuffer buf = CreateVarintBuffer({1, 3, 8, 9});
    VarintIteratorEndBound iterator(buf);

    PositionTermEntry entry(&iterator);
    REQUIRE(entry.Values() == std::vector<uint32_t>{1, 2, 5, 1});
    REQUIRE(entry.VInts().Size() == 4);
    REQUIRE(entry.PackWriters().size() == 0);

    SECTION("Dump it") {
      PositionDumper dumper("/tmp/tmp.pos.dumper");
      EntryMetadata metadata = dumper.Dump(entry);
      REQUIRE(dumper.CurrentOffset() == 4); // Only the vints are in it
      
      REQUIRE(metadata.PackOffSize() == 0); 
      REQUIRE(metadata.VIntsSize() == 1); 
    }
  }

  SECTION("More than one packed block") {
    std::vector<int> vec;
    std::vector<uint32_t> deltas;
    int prev = 0;
    for (int i = 0; i < 200; i++) {
      int delta = i % 7;
      deltas.push_back(delta);

      int num = prev + delta;
      vec.push_back(num); 
      prev = num;
    }
    VarintBuffer buf = CreateVarintBuffer(vec);
    VarintIteratorEndBound iterator(buf);

    PositionTermEntry entry(&iterator);
    REQUIRE(entry.Values() == deltas);
    REQUIRE(entry.VInts().Size() == (200 - PackedIntsWriter::PACK_SIZE));
    REQUIRE(entry.PackWriters().size() == 1);

    SECTION("Dump it and read it") {
      // Dump it
      PositionDumper dumper("/tmp/tmp.pos.dumper");
      EntryMetadata metadata = dumper.Dump(entry);
      
      REQUIRE(metadata.PackOffSize() == 1); 
      REQUIRE(metadata.VIntsSize() == 1); 
      dumper.Flush();
      dumper.Close();

      // Read it
      int fd;
      char *addr;
      size_t file_length;
      utils::MapFile("/tmp/tmp.pos.dumper", &addr, &fd, &file_length);

      PackedIntsReader reader((uint8_t *)addr + metadata.PackOffs()[0]);
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        REQUIRE(reader.Get(i) == deltas[i]);
      }

      utils::UnmapFile(addr, fd, file_length);
    }
  }
}


TEST_CASE( "Test Position Dumper", "[qqflash]" ) {
  SECTION("initialize and destruct") {
    PositionDumper dumper("/tmp/tmp.pos.dumper");
  }
  
  SECTION("It returns the right metadata about the dumpped entry") {
    PositionDumper dumper("/tmp/tmp.pos.dumper");
  }

  SECTION("Get the current position") {
    PositionDumper dumper("/tmp/tmp.pos.dumper");
    REQUIRE(dumper.CurrentOffset() == 0);
  }
}


TEST_CASE( "Dumping Engine", "[qqflash]" ) {
  FlashEngineDumper engine;
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


TEST_CASE( "Write and Read works", "[qqflash]" ) {
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
    entry.AddPostingColumn({7});

    REQUIRE(entry.Values() == std::vector<uint32_t>{7});
    REQUIRE(entry.PostingSizes() == std::vector<int>{1});

    PostingLocationTable table = entry.LocationTable();

    REQUIRE(table.NumRows() == 1);
    REQUIRE(table[0].packed_block_idx == 0);
    REQUIRE(table[0].in_block_idx == 0);
  }

  SECTION("Multiple postings") {
    GeneralTermEntry entry;  
    entry.AddPostingColumn({7});
    entry.AddPostingColumn({9, 10});
    entry.AddPostingColumn({11, 18});

    REQUIRE(entry.Values() == std::vector<uint32_t>{7, 9, 10, 11, 18});
    REQUIRE(entry.PostingSizes() == std::vector<int>{1, 2, 2});

    PostingLocationTable table = entry.LocationTable();

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
    entry.AddPostingColumn(vec);
    REQUIRE(entry.Values() == vec);

    TermEntryContainer container = entry.GetContainer(true);
    REQUIRE(container.PackWriters().size() == 1);
    REQUIRE(container.VInts().Size() == (200 - PackedIntsWriter::PACK_SIZE));
  }
}














