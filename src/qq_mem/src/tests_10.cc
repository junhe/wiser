#include "catch.hpp"

#include "flash_engine_dumper.h"


TEST_CASE( "Dumping Engine", "[qqflash]" ) {
  auto engine = CreateSearchEngine("flash_engine_dumper");
  REQUIRE(engine->TermCount() == 0);
  engine->LoadLocalDocuments("src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");

  SECTION("Check loading") {
    REQUIRE(engine->TermCount() > 0);
    auto result = engine->Search(SearchQuery({"prefix"}));
    std::cout << result.ToStr() << std::endl;
    REQUIRE(result.Size() > 0);

    {
      SearchQuery query({"solar", "body"}, true);
      result = engine->Search(query);
      std::cout << result.ToStr() << std::endl;
      REQUIRE(result.Size() == 0); // only solar is a valid term
    }
  }
}






