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
    REQUIRE(entry.Deltas() == std::vector<uint32_t>{1, 2, 5, 1});
    REQUIRE(entry.VInts().Size() == 4);
    REQUIRE(entry.PackWriters().size() == 0);

    SECTION("Dump it") {
      PositionDumper dumper("/tmp/tmp.pos.dumper");
      PositionMetadata metadata = dumper.Dump(entry);
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
    REQUIRE(entry.Deltas() == deltas);
    REQUIRE(entry.VInts().Size() == (200 - PackedIntsWriter::PACK_SIZE));
    REQUIRE(entry.PackWriters().size() == 1);

    SECTION("Dump it and read it") {
      // Dump it
      PositionDumper dumper("/tmp/tmp.pos.dumper");
      PositionMetadata metadata = dumper.Dump(entry);
      
      REQUIRE(metadata.PackOffSize() == 1); 
      REQUIRE(metadata.VIntsSize() == 1); 
      dumper.Flush();
      dumper.Close();

      // Read it
      int fd;
      char *addr;
      size_t file_length;
      utils::MapFile("/tmp/tmp.pos.dumper", &addr, &fd, &file_length);

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









