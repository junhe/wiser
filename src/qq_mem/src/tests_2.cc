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

    SECTION("GetRow()") {
      const IntersectionResult::row_dict_t *row = res.GetRow(0);
      auto row_it = res.row_cbegin();  
      REQUIRE(&row_it->second == row);
    }
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


TEST_CASE( "Intersect, score and sort in one place", "[intersect]" ) {
  IntersectionResult result;  
  DocLengthStore doc_lengths;

  PostingList_Vec<PostingSimple> pl01("hello");   
  for (int i = 0; i < 10; i++) {
    pl01.AddPosting(PostingSimple(i, 1, Positions{28}));
  }

  PostingList_Vec<PostingSimple> pl02("world");   
  for (int i = 5; i < 10; i++) {
    pl02.AddPosting(PostingSimple(i, 1, Positions{28}));
  }

  for (int i = 0; i < 10; i++) {
    doc_lengths.AddLength(i, 20);
  }

  std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
  intersect_score_and_sort<PostingSimple>(lists, doc_lengths, 10);
}


TEST_CASE( "score_terms_in_doc()", "[score]" ) {
  // Assuming an engine that has the following documents
  //
  // hello world
  // hello wisconsin
  // hello world big world
  int n_total_docs_in_index = 3;
  qq_float avg_doc_length_in_index = (2 + 2 + 4) / 3.0;

  SECTION("Query wisconsin") {
    // This is document "hello wisconsin"
    int length_of_this_doc = 2;
    PostingList_Vec<PostingSimple> pl01("wisconsin");   
    pl01.AddPosting(PostingSimple(0, 1, Positions{28}));

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01};
    std::vector<PostingList_Vec<PostingSimple>::iterator_t> posting_iters{0};
    std::vector<int> doc_freqs_of_terms{1};

    qq_float doc_score = calc_doc_score_for_a_query<PostingSimple>(
          lists, 
          posting_iters,
          doc_freqs_of_terms,
          n_total_docs_in_index,
          avg_doc_length_in_index,
          length_of_this_doc);
    REQUIRE(utils::format_double(doc_score, 3) == "1.09");
  }

  SECTION("Query hello") {
    // This is document "hello world"
    int length_of_this_doc = 2;

    PostingList_Vec<PostingSimple> pl01("hello");   
    pl01.AddPosting(PostingSimple(0, 1, Positions{28}));

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01};
    std::vector<PostingList_Vec<PostingSimple>::iterator_t> posting_iters{0};
    std::vector<int> doc_freqs_of_terms{3};

    qq_float doc_score = calc_doc_score_for_a_query<PostingSimple>(
          lists, 
          posting_iters,
          doc_freqs_of_terms,
          n_total_docs_in_index,
          avg_doc_length_in_index,
          length_of_this_doc);
    REQUIRE(utils::format_double(doc_score, 3) == "0.149");
  }

  SECTION("Query hello+world") {
    // This is document "hello world"
    int length_of_this_doc = 2;

    PostingList_Vec<PostingSimple> pl01("hello");   
    pl01.AddPosting(PostingSimple(0, 1, Positions{28}));

    PostingList_Vec<PostingSimple> pl02("world");   
    pl02.AddPosting(PostingSimple(0, 1, Positions{28}));

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<PostingList_Vec<PostingSimple>::iterator_t> posting_iters{0, 0};
    std::vector<int> doc_freqs_of_terms{3, 2};

    qq_float doc_score = calc_doc_score_for_a_query<PostingSimple>(
          lists, 
          posting_iters,
          doc_freqs_of_terms,
          n_total_docs_in_index,
          avg_doc_length_in_index,
          length_of_this_doc);
    REQUIRE(utils::format_double(doc_score, 3) == "0.672");
  }
}











