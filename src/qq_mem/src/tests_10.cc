#include "catch.hpp"

#include "flash_engine_dumper.h"


TEST_CASE( "Dumping Engine", "[qqflash]" ) {
  FlashEngineDumper engine;
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");

  SECTION("Check loading") {
    REQUIRE(engine.TermCount() > 0);
  }
}






