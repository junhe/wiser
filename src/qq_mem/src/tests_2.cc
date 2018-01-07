#include "catch.hpp"

#include "unifiedhighlighter.h"
#include "intersect.h"
#include "utils.h"

TEST_CASE( "SimpleHighlighter works", "[highlighter]" ) {
  std::string doc_str = "hello world";

  Offsets hello_offsets;
  hello_offsets.emplace_back( Offset{0, 4} ); 

  Offsets world_offsets;
  world_offsets.emplace_back( Offset{6, 10} ); 

  SECTION("One-term query") {
    OffsetsEnums enums;
    enums.push_back(Offset_Iterator(hello_offsets));

    SimpleHighlighter highlighter;
    auto s = highlighter.highlightOffsetsEnums(enums, 2, doc_str);
    REQUIRE(s == "<b>hello<\\b> world\n");
  }

  SECTION("Two-term query") {
    OffsetsEnums enums;
    enums.push_back(Offset_Iterator(hello_offsets));
    enums.push_back(Offset_Iterator(world_offsets));

    SimpleHighlighter highlighter;
    auto s = highlighter.highlightOffsetsEnums(enums, 2, doc_str);
    REQUIRE(s == "<b>hello<\\b> <b>world<\\b>\n");
  }
}

TEST_CASE( "IntersectionResult works", "[intersection]" ) {
  IntersectionResult res;

  SECTION("Doc count works") {
    res.SetDocCount("term1", 8);
    REQUIRE(res.GetDocCount("term1") == 8);
  }

  SECTION("Basic Posting setting and getting work") {
    RankingPosting *p0, *p1, *p2;
    const QqMemPostingService *p;

    res.SetPosting(0, "term1", p0);
    res.SetPosting(1, "term1", p1);
    res.SetPosting(2, "term1", p2);

    auto row_it = res.row_cbegin();
    auto col_it = IntersectionResult::col_cbegin(row_it);
    p = IntersectionResult::GetPosting(col_it);
    REQUIRE(IntersectionResult::GetCurDocId(row_it) == 0);
    REQUIRE(p == p0);

    col_it++;
    REQUIRE(col_it == IntersectionResult::col_cend(row_it));


    row_it++;
    col_it = IntersectionResult::col_cbegin(row_it);
    p = IntersectionResult::GetPosting(col_it);
    REQUIRE(IntersectionResult::GetCurDocId(row_it) == 1);
    REQUIRE(p == p1);

    row_it++;
    col_it = IntersectionResult::col_cbegin(row_it);
    p = IntersectionResult::GetPosting(col_it);
    REQUIRE(IntersectionResult::GetCurDocId(row_it) == 2);
    REQUIRE(p == p2);
  }
}


TEST_CASE( "Extract offset pairs from a string", "[utils]" ) {
  SECTION("Adding entry") {
    std::map<Term, OffsetPairs> result;
    utils::add_term_offset_entry(&result, "hello", 5);
    utils::add_term_offset_entry(&result, "world", 11);
    utils::add_term_offset_entry(&result, "hello", 17);

    REQUIRE(result["hello"].size() == 2);
    REQUIRE(result["hello"][0] == std::make_tuple(0, 4));
    REQUIRE(result["hello"][1] == std::make_tuple(12, 16));

    REQUIRE(result["world"].size() == 1);
    REQUIRE(result["world"][0] == std::make_tuple(6, 10));
  }

  SECTION("hello world hello") {
    auto result = utils::extract_offset_pairs("hello world hello");

    REQUIRE(result.size() == 2);

    REQUIRE(result["hello"].size() == 2);
    REQUIRE(result["hello"][0] == std::make_tuple(0, 4));
    REQUIRE(result["hello"][1] == std::make_tuple(12, 16));

    REQUIRE(result["world"].size() == 1);
    REQUIRE(result["world"][0] == std::make_tuple(6, 10));
  }

  SECTION("empty string") {
    auto result = utils::extract_offset_pairs("");

    REQUIRE(result.size() == 0);
  }

  SECTION("one letter string") {
    auto result = utils::extract_offset_pairs("h");

    REQUIRE(result.size() == 1);
    REQUIRE(result["h"].size() == 1);
    REQUIRE(result["h"][0] == std::make_tuple(0, 0));
  }

  SECTION("space only string") {
    auto result = utils::extract_offset_pairs("    ");

    REQUIRE(result.size() == 0);
  }

  SECTION("pre and suffix spaces") {
    auto result = utils::extract_offset_pairs(" h ");

    REQUIRE(result.size() == 1);
    REQUIRE(result["h"].size() == 1);
    REQUIRE(result["h"][0] == std::make_tuple(1, 1));
  }

  SECTION("double space seperator") {
    auto result = utils::extract_offset_pairs("h  h");

    REQUIRE(result.size() == 1);

    REQUIRE(result["h"].size() == 2);
    REQUIRE(result["h"][0] == std::make_tuple(0, 0));
    REQUIRE(result["h"][1] == std::make_tuple(3, 3));
  }

}



