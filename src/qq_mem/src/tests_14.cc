#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "utils.h"


TEST_CASE( "Dumping 1-word Engine", "[qqflash][dump0]" ) {
  std::string dir_path = "/tmp/1-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine(dir_path);
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/one_word_with_position", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine.TermCount() == 1);

  SECTION("Dumping inverted index") {
    engine.DumpInvertedIndex();
  }
}

TEST_CASE( "Dumping 5-word Engine", "[qqflash][dump1]" ) {
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


