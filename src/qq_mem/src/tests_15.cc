#include "catch.hpp"

#include <algorithm>

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
    SearchResult result = engine.Search(SearchQuery({"a"}, true));
    REQUIRE(result.Size() == 3);
    std::cout << result.ToStr();

    for (int i = 0; i < result.Size(); i++) {
      if (result[i].doc_id == 0) {
        REQUIRE(result[i].snippet == ""); //TODO: this should be fixed
      } else if (result[i].doc_id == 1) {
        REQUIRE(result[i].snippet == "<b>a <\\b>b\n");
      } else {
        REQUIRE(result[i].snippet == "<b>a <\\b>b c\n");
      }
    }

    std::cout << ">>>>>>>>>>>>>>>> b" << std::endl;
    result = engine.Search(SearchQuery({"b"}, true));
    REQUIRE(result.Size() == 2);
    std::cout << result.ToStr();

    for (int i = 0; i < result.Size(); i++) {
      if (result[i].doc_id == 1) {
        // REQUIRE(result[i].snippet == "a <b>b <\\b>\n"); //Failed. TODO
      } else if (result[i].doc_id == 2) {
        REQUIRE(result[i].snippet == "a <b>b <\\b>c\n");
      }
    }

    std::cout << ">>>>>>>>>>>>>>>> c" << std::endl;
    result = engine.Search(SearchQuery({"c"}, true));
    REQUIRE(result.Size() == 1);
    std::cout << result.ToStr();

    // REQUIRE(result[0].snippet == "a b <b>c <\\b>\n"); // Fail, TODO

    result = engine.Search(SearchQuery({"d"}, true));
    REQUIRE(result.Size() == 0);
  }

  SECTION("Two-term non-phrase query") {
    std::cout << "22222222222222222222222222222222222222222222222\n";
    SearchResult result = engine.Search(SearchQuery({"a", "b"}, true));
    std::cout << result.ToStr();
  }


}


TEST_CASE( "tests two intersection ngine", "[qqflash][two]" ) {
  std::string dir_path = "/tmp/3-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine_dumper(dir_path);
  REQUIRE(engine_dumper.TermCount() == 0);
  engine_dumper.LoadLocalDocuments("src/testdata/iter_test_3_docs", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine_dumper.TermCount() == 3);

  engine_dumper.Dump();

  VacuumEngine engine(dir_path);

  SECTION("Two-term non-phrase query") {
    SearchResult result = engine.Search(SearchQuery({"a", "b"}, true));
    REQUIRE(result.Size() == 2);
    std::vector<DocIdType> ids{result[0].doc_id, result[1].doc_id};
    std::sort(ids.begin(), ids.end());
    REQUIRE(ids == std::vector<DocIdType>{1, 2});
  }

  SECTION("Two-term phrase query") {
    SearchQuery query({"a", "b"}, true);
    query.is_phrase = true;

    SearchResult result = engine.Search(query);
    REQUIRE(result.Size() == 2);
    std::vector<DocIdType> ids{result[0].doc_id, result[1].doc_id};
    std::sort(ids.begin(), ids.end());
    REQUIRE(ids == std::vector<DocIdType>{1, 2});
  }

  SECTION("three-term phrase query") {
    SearchQuery query({"a", "b", "c"}, true);
    query.is_phrase = true;

    SearchResult result = engine.Search(query);
    std::cout << result.ToStr();
    REQUIRE(result.Size() == 1);
    std::vector<DocIdType> ids{result[0].doc_id};
    REQUIRE(ids == std::vector<DocIdType>{2});
  }
}

TEST_CASE( "Testing 5 long docs", "[qqflash][dump5]" ) {
  std::string dir_path = "/tmp/3-doc-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine_dumper(dir_path);
  REQUIRE(engine_dumper.TermCount() == 0);
  engine_dumper.LoadLocalDocuments("./src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine_dumper.TermCount() > 100);

  engine_dumper.Dump();

  VacuumEngine engine(dir_path);

  SECTION("one-term query") {
    SearchResult result = engine.Search(SearchQuery({"1860"}, true));
    REQUIRE(result.Size() == 1);
    std::vector<DocIdType> ids{result[0].doc_id};
    std::sort(ids.begin(), ids.end());
    REQUIRE(ids == std::vector<DocIdType>{0});
  }

  SECTION("Two-term phrase query (not exist)") {
    SearchQuery query({"a", "b"}, true);
    query.is_phrase = true;

    SearchResult result = engine.Search(query);
    REQUIRE(result.Size() == 0);
  }

  SECTION("Two-term phrase query") {
    SearchQuery query({"anarchist", "movement"}, true);
    query.is_phrase = true;

    SearchResult result = engine.Search(query);
    REQUIRE(result.Size() == 1);
  }
}


TEST_CASE( "Testing 5 long docs (comparing QQMem and Vacuum", "[qqflash][qqvacuum]" ) {
  // Create vacuum
  std::string dir_path = "/tmp/3-doc-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine_dumper(dir_path);
  REQUIRE(engine_dumper.TermCount() == 0);
  engine_dumper.LoadLocalDocuments("./src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine_dumper.TermCount() > 100);
  engine_dumper.Dump();

  VacuumEngine vacuum_engine(dir_path);


  QqMemEngineDelta qq_engine;
  qq_engine.LoadLocalDocuments("./src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");
  REQUIRE(qq_engine.TermCount() > 100);

  REQUIRE(vacuum_engine.TermCount() == qq_engine.TermCount());

  // "all"
  SearchQuery query({"all"});
  auto v_result = vacuum_engine.Search(query);
  std::cout << "v-------\n";
  std::cout << v_result.ToStr();
  auto q_result = qq_engine.Search(query);
  std::cout << "q-------\n";
  std::cout << q_result.ToStr();
  REQUIRE(v_result == q_result);
}



// TEST_CASE( "Load qq mem dump and dump to vacuum format", "[load]" ) {
  // std::string dir_path = "/tmp/3-doc-engine";
  // utils::PrepareDir(dir_path);
  // FlashEngineDumper engine_dumper(dir_path);
  // REQUIRE(engine_dumper.TermCount() == 0);
  // engine_dumper.LoadQqMemDump("/mnt/ssd/big-engine-dump-compressed-text");
  // REQUIRE(engine_dumper.TermCount() > 100);

  // engine_dumper.Dump();
// }







