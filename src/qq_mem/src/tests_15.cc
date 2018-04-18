#include "catch.hpp"

#include "test_helpers.h"
#include "flash_iterators.h"
#include "vacuum_engine.h"
#include "utils.h"

TEST_CASE( "Initializing 3-word vacuum engine", "[qqflash][vengine]" ) {
  std::string dir_path = "/tmp/3-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine_dumper(dir_path);
  REQUIRE(engine_dumper.TermCount() == 0);
  engine_dumper.LoadLocalDocuments("src/testdata/iter_test_3_docs", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine_dumper.TermCount() == 3);

  engine_dumper.Dump();

  VacuumEngine engine(dir_path);

  // Sanity check
  REQUIRE(engine.TermCount() == 3);
  auto pl_sizes = engine.PostinglistSizes({"a", "b", "c"});
  REQUIRE(pl_sizes["a"] == 3);
  REQUIRE(pl_sizes["b"] == 2);
  REQUIRE(pl_sizes["c"] == 1);

  SECTION("Single term query") {
    std::cout << ">>>>>>>>>>>>>>>> a" << std::endl;
    SearchResult result = engine.Search(SearchQuery({"a"}));
    REQUIRE(result.Size() == 3);
    std::cout << result.ToStr();

    std::cout << ">>>>>>>>>>>>>>>> b" << std::endl;
    result = engine.Search(SearchQuery({"b"}, true));
    REQUIRE(result.Size() == 2);
    std::cout << result.ToStr();

    std::cout << ">>>>>>>>>>>>>>>> c" << std::endl;
    result = engine.Search(SearchQuery({"c"}, true));
    REQUIRE(result.Size() == 1);
    std::cout << result.ToStr();

    result = engine.Search(SearchQuery({"d"}, true));
    REQUIRE(result.Size() == 0);
  }
}




