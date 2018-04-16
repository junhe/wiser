#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "vacuum_engine.h"
#include "utils.h"


TEST_CASE( "Dumping 1-word Engine", "[qqflash][dump0]" ) {
  std::string dir_path = "/tmp/1-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine(dir_path);
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/one_word_with_position", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine.TermCount() == 1);

  // Dump the engine
  engine.DumpInvertedIndex();

  SECTION("Load inverted index") {
    VacuumInvertedIndex index(
        "/tmp/1-word-engine/my.tip", "/tmp/1-word-engine/my.vaccum");
    REQUIRE(index.FindPostingListOffset("a") == 0);
    REQUIRE(index.FindPostingListOffset("b") == -1);
    REQUIRE(index.NumTerms() == 1);

    SECTION("Get the posting list iterator") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"a"});
      REQUIRE(iters.size() == 1);
    }

    SECTION("Get the posting list iterator, 2") {
      std::vector<VacuumPostingListIterator> iters 
        = index.FindIteratorsSolid({"b"});
      REQUIRE(iters.size() == 0);
    }
  }
}


TEST_CASE( "Dumping 5-word Engine", "[qqflash][dump1]" ) {
  std::string dir_path = "/tmp/5-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine(dir_path);
  REQUIRE(engine.TermCount() == 0);
  engine.LoadLocalDocuments("src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");

  REQUIRE(engine.TermCount() > 0);
  engine.DumpInvertedIndex();

  SECTION("Load inverted index") {
    VacuumInvertedIndex index(
        "/tmp/5-word-engine/my.tip", "/tmp/5-word-engine/my.vaccum");
    REQUIRE(index.NumTerms() > 100);
  }
}


