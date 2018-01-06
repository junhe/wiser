#include "catch.hpp"

#include "unifiedhighlighter.h"
#include "intersect.h"

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



