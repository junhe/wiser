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






