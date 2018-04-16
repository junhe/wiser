#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "utils.h"

TEST_CASE( "Dumping Engine", "[qqflash][engine_dump]" ) {
  FlashEngineDumper engine("/tmp");
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");

  SECTION("Check loading") {
    REQUIRE(engine.TermCount() > 0);
  }

  SECTION("Dumping inverted index") {
    engine.DumpInvertedIndex();
  }
}


