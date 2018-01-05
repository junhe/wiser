#include "catch.hpp"

#include "unifiedhighlighter.h"

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

