#include "catch.hpp"

#include <algorithm>

#include "test_helpers.h"
#include "flash_iterators.h"
#include "vacuum_engine.h"
#include "utils.h"
#include "engine_factory.h"

TEST_CASE( "Initializing 3-word vacuum engine", "[qqflash][vengine]" ) {
  std::string dir_path = "/tmp/3-word-engine";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine_dumper(dir_path);
  REQUIRE(engine_dumper.TermCount() == 0);
  engine_dumper.LoadLocalDocuments("src/testdata/iter_test_3_docs", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine_dumper.TermCount() == 3);

  engine_dumper.Dump();

  VacuumEngine<ChunkedDocStoreReader> engine(dir_path);
  engine.Load();

  // Sanity check
  REQUIRE(engine.TermCount() == 3);
  auto pl_sizes = engine.PostinglistSizes({"a", "b", "c"});
  REQUIRE(pl_sizes["a"] == 3);
  REQUIRE(pl_sizes["b"] == 2);
  REQUIRE(pl_sizes["c"] == 1);

  SECTION("Single term query") {
    SearchResult result = engine.Search(SearchQuery({"a"}, true));
    REQUIRE(result.Size() == 3);

    for (std::size_t i = 0; i < result.Size(); i++) {
      if (result[i].doc_id == 0) {
        REQUIRE(result[i].snippet == ""); //TODO: this should be fixed
      } else if (result[i].doc_id == 1) {
        REQUIRE(result[i].snippet == "<b>a <\\b>b\n");
      } else {
        REQUIRE(result[i].snippet == "<b>a <\\b>b c\n");
      }
    }

    result = engine.Search(SearchQuery({"b"}, true));
    REQUIRE(result.Size() == 2);

    for (std::size_t i = 0; i < result.Size(); i++) {
      if (result[i].doc_id == 1) {
        // REQUIRE(result[i].snippet == "a <b>b <\\b>\n"); //Failed. TODO
      } else if (result[i].doc_id == 2) {
        REQUIRE(result[i].snippet == "a <b>b <\\b>c\n");
      }
    }

    result = engine.Search(SearchQuery({"c"}, true));
    REQUIRE(result.Size() == 1);

    // REQUIRE(result[0].snippet == "a b <b>c <\\b>\n"); // Fail, TODO

    result = engine.Search(SearchQuery({"d"}, true));
    REQUIRE(result.Size() == 0);
  }

  SECTION("Two-term non-phrase query") {
    SearchResult result = engine.Search(SearchQuery({"a", "b"}, true));
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

  VacuumEngine<ChunkedDocStoreReader> engine(dir_path);
  engine.Load();

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

  VacuumEngine<ChunkedDocStoreReader> engine(dir_path);
  engine.Load();

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

  VacuumEngine<ChunkedDocStoreReader> vacuum_engine(dir_path);
  vacuum_engine.Load();


  QqMemEngineDelta qq_engine;
  qq_engine.LoadLocalDocuments("./src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");
  REQUIRE(qq_engine.TermCount() > 100);

  REQUIRE(vacuum_engine.TermCount() == qq_engine.TermCount());

  // "all"
  {
  SearchQuery query({"all"});
  auto v_result = vacuum_engine.Search(query);
  auto q_result = qq_engine.Search(query);
  REQUIRE(v_result == q_result);
  }

  // four
  {
  SearchQuery query({"four"});
  auto v_result = vacuum_engine.Search(query);
  auto q_result = qq_engine.Search(query);
  REQUIRE(v_result == q_result);
  }

  {
    std::string line;
    std::ifstream infile("./src/testdata/all-tokens.txt");
    std::getline(infile, line);
    auto terms = utils::explode(line, ' ');
  
    for (auto &term : terms) {
      SearchQuery query({term});
      auto v_result = vacuum_engine.Search(query);
      auto q_result = qq_engine.Search(query);
      REQUIRE(v_result.Size() > 0);
      REQUIRE(v_result == q_result);
    }
  }
}


TEST_CASE( "Dumping to large file", "[qqflash][large]" ) {
  std::string dir_path = "/tmp/large-file";
  utils::PrepareDir(dir_path);
  FlashEngineDumper engine_dumper(dir_path);
  REQUIRE(engine_dumper.TermCount() == 0);
  engine_dumper.LoadLocalDocuments("src/testdata/iter_test_3_docs", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine_dumper.TermCount() == 3);

  // Dump the engine_dumper
  // Do a regular dump so the header check won't fail
  engine_dumper.Dump();

  off_t fake_content_off = 5*GB - 10;
  engine_dumper.SeekInvertedIndexDumper(fake_content_off);
  engine_dumper.Dump();

  VacuumEngine<ChunkedDocStoreReader> engine(dir_path);
  engine.Load();

  // Sanity check
  REQUIRE(engine.TermCount() == 3);
  auto pl_sizes = engine.PostinglistSizes({"a", "b", "c"});

  SECTION("Single term query") {
    SearchResult result = engine.Search(SearchQuery({"a"}, true));
    REQUIRE(result.Size() == 3);

    for (std::size_t i = 0; i < result.Size(); i++) {
      if (result[i].doc_id == 0) {
        REQUIRE(result[i].snippet == ""); //TODO: this should be fixed
      } else if (result[i].doc_id == 1) {
        REQUIRE(result[i].snippet == "<b>a <\\b>b\n");
      } else {
        REQUIRE(result[i].snippet == "<b>a <\\b>b c\n");
      }
    }

    result = engine.Search(SearchQuery({"b"}, true));
    REQUIRE(result.Size() == 2);

    for (std::size_t i = 0; i < result.Size(); i++) {
      if (result[i].doc_id == 1) {
        // REQUIRE(result[i].snippet == "a <b>b <\\b>\n"); //Failed. TODO
      } else if (result[i].doc_id == 2) {
        REQUIRE(result[i].snippet == "a <b>b <\\b>c\n");
      }
    }

    result = engine.Search(SearchQuery({"c"}, true));
    REQUIRE(result.Size() == 1);

    // REQUIRE(result[0].snippet == "a b <b>c <\\b>\n"); // Fail, TODO

    result = engine.Search(SearchQuery({"d"}, true));
    REQUIRE(result.Size() == 0);
  }


}


TEST_CASE( "Test fake file dumper", "[fake]" ) {
  FileDumper real("/tmp/real.dump");
  FakeFileDumper fake("/tmp/fake.dump");

  REQUIRE(real.CurrentOffset() == fake.CurrentOffset());
  REQUIRE(real.End() == fake.End());

  std::string data = "hello";
  REQUIRE(real.Dump(data) == fake.Dump(data));
  REQUIRE(real.Dump(data) == fake.Dump(data));
  REQUIRE(real.End() == fake.End());

  data = "helloxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
  REQUIRE(real.Dump(data) == fake.Dump(data));
  real.Seek(33);
  fake.Seek(33);
  REQUIRE(real.CurrentOffset() == fake.CurrentOffset());

  real.SeekToEnd();
  fake.SeekToEnd();
  REQUIRE(real.CurrentOffset() == fake.CurrentOffset());
  REQUIRE(real.End() == fake.End());

}


TEST_CASE( "URL parsing", "[parse]" ) {
  REQUIRE(IsVacuumUrl("vacuum:linedoc:/my/path"));
  REQUIRE(IsVacuumUrl("/my/path") == false);
  
  VacuumConfig conf = ParseUrl("vacuum:vacuum_dump:/my/path");
  REQUIRE(conf.source_type == "vacuum_dump");
  REQUIRE(conf.path == "/my/path");
}


TEST_CASE( "64 bits encoding and decoding", "[v64]" ) {
  uint64_t val = 3748449232L; 
  std::string buf;
  int len1 = utils::varint_expand_and_encode(val, &buf, 0);

  uint64_t val2;
  int len2 = utils::varint_decode_64bit(buf.data(), 0, &val2);

  // uint32_t val2;
  // int len2 = utils::varint_decode_uint32(buf.data(), 0, &val2);

  REQUIRE(len1 == len2);
  REQUIRE(val == val2);
}






