#include "catch.hpp"

#include "utils.h"
#include "compression.h"
#include "posting.h"
#include "posting_list_delta.h"
#include "posting_list_vec.h"
#include "intersect.h"

#include "test_helpers.h"


typedef StandardPosting PostingWO;

TEST_CASE( "QueryProcessor works", "[engine0]" ) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < 10; i++) {
    offset_pairs.push_back(std::make_tuple(1, 2)); 
  }

  PostingListDelta pl01("hello");
  PostingListDelta pl02("world");
  PostingListDelta pl03("again");

  for (int i = 0; i < 5; i++) {
    pl01.AddPosting(PostingWO(i, 3, offset_pairs));
    pl02.AddPosting(PostingWO(i, 3, offset_pairs));
    pl03.AddPosting(PostingWO(i, 3, offset_pairs));
  }

  DocLengthStore store;
  for (int i = 0; i < 5; i++) {
    store.AddLength(i, (5 - i) * 10);
  }

  SECTION("Find top 5") {
    IteratorPointers iterators;
    iterators.push_back(std::move(pl01.Begin()));
    iterators.push_back(std::move(pl02.Begin()));

    QueryProcessor processor(&iterators, store, 100, 5);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 5);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3, 2, 1, 0});
  }

  SECTION("Find top 2") {
    IteratorPointers iterators;
    iterators.push_back(std::move(pl01.Begin()));
    iterators.push_back(std::move(pl02.Begin()));

    QueryProcessor processor(&iterators, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 within one postinglist") {
    IteratorPointers iterators;
    iterators.push_back(std::move(pl01.Begin()));

    QueryProcessor processor(&iterators, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 with three posting lists") {
    IteratorPointers iterators;
    iterators.push_back(std::move(pl01.Begin()));
    iterators.push_back(std::move(pl02.Begin()));
    iterators.push_back(std::move(pl03.Begin()));

    QueryProcessor processor(&iterators, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }
}


TEST_CASE( "PostingList_Vec iterator", "[posting_list]" ) {
  auto pl = create_posting_list_standard(10); 

  SECTION("Test Advance()") {
    auto it = pl.Begin();

    for (int i = 0; i < 10; i++, it->Advance()) {
      REQUIRE(it->IsEnd() == false);
      REQUIRE(it->DocId() == i);
      REQUIRE(it->TermFreq() == i * 2);
    }
    REQUIRE(it->IsEnd() == true);
  }

  SECTION("Test Offset Iterator") {
    auto it = pl.Begin();
    std::unique_ptr<OffsetPairsIteratorService> off_it = it->OffsetPairsBegin();

    for (int i = 0; i < 5; i++) {
      OffsetPair pair;
      off_it->Pop(&pair);
      REQUIRE(std::get<0>(pair) == i * 2);
      REQUIRE(std::get<1>(pair) == i * 2 + 1);
    }
    REQUIRE(off_it->IsEnd());
  }
}

TEST_CASE( "VarintIterator", "[compression]" ) {
  VarintBuffer buf;
  buf.Append(1);
  buf.Append(5);
  buf.Append(9);

  SECTION("Regular operations") {
    VarintIterator it(buf, 3);

    REQUIRE(it.IsEnd() == false);
    REQUIRE(it.Pop() == 1);

    REQUIRE(it.IsEnd() == false);
    REQUIRE(it.Pop() == 5);

    REQUIRE(it.IsEnd() == false);
    REQUIRE(it.Pop() == 9);

    REQUIRE(it.IsEnd() == true);
  }
}

VarintBuffer create_varint_buffer(std::vector<uint32_t> vec) {
  VarintBuffer buf;
  for (auto k : vec) {
    buf.Append(k);
  }

  return buf;
}

VarintIterators create_iterators(std::vector<VarintBuffer *> buffers, std::vector<int> sizes) {
  VarintIterators iterators;
  for (int i = 0; i < buffers.size(); i++) {
    iterators.emplace_back(*buffers[i], sizes[i]);
  }
  return iterators;
}

TEST_CASE( "PhraseQueryProcessor", "[engine]" ) {
  SECTION("Simple 001") {
    // 3, 4 is a match
    VarintBuffer buf01 = create_varint_buffer(std::vector<uint32_t>{1, 3, 5});
    VarintBuffer buf02 = create_varint_buffer(std::vector<uint32_t>{4});
    auto iterators = create_iterators({&buf01, &buf02}, {3, 1});

    PhraseQueryProcessor qp(&iterators);
   
    SECTION("Regular simple case") {
      auto positions = qp.Process();
      REQUIRE(positions == Positions{3});
    }

    SECTION("FindMaxAdjustedLastPopped() and MovePoppedBeyond()") {
      bool found;
      qp.InitializeLastPopped();
      auto max = qp.FindMaxAdjustedLastPopped();  
      REQUIRE(max == 3);

      found = qp.MovePoppedBeyond(1);
      REQUIRE(found == true);
      max = qp.FindMaxAdjustedLastPopped();  
      REQUIRE(max == 3);

      // stay where you are
      found = qp.MovePoppedBeyond(1);
      REQUIRE(found == true);
      max = qp.FindMaxAdjustedLastPopped();  
      REQUIRE(max == 3);

      found = qp.MovePoppedBeyond(3);
      REQUIRE(found == true);
      max = qp.FindMaxAdjustedLastPopped();  
      REQUIRE(max == 3);
      REQUIRE(qp.IsPoppedMatch(3) == true);

      found = qp.MovePoppedBeyond(5);
      REQUIRE(found == false); // coundn't find 5 in the second list
    }
  }

  SECTION("Two empty lists") {
    VarintBuffer buf01 = create_varint_buffer(std::vector<uint32_t>{});
    VarintBuffer buf02 = create_varint_buffer(std::vector<uint32_t>{});
    auto iterators = create_iterators({&buf01, &buf02}, {0, 0});

    PhraseQueryProcessor qp(&iterators);
    auto positions = qp.Process();

    REQUIRE(positions == Positions{});
  }

  SECTION("No matches") {
    VarintBuffer buf01 = create_varint_buffer({1, 8, 20});
    VarintBuffer buf02 = create_varint_buffer({0, 7, 19});
    auto iterators = create_iterators({&buf01, &buf02}, {3, 3});

    PhraseQueryProcessor qp(&iterators);
    auto positions = qp.Process();

    REQUIRE(positions == Positions{});
  }
 
  SECTION("Bad positions") {
    // Two terms cannot be at the same position
    VarintBuffer buf01 = create_varint_buffer({0});
    VarintBuffer buf02 = create_varint_buffer({0});
    auto iterators = create_iterators({&buf01, &buf02}, {1, 1});

    PhraseQueryProcessor qp(&iterators);
    auto positions = qp.Process();

    REQUIRE(positions == Positions{});
  }
 
  SECTION("One list is empty") {
    VarintBuffer buf01 = create_varint_buffer({10});
    VarintBuffer buf02 = create_varint_buffer({});
    auto iterators = create_iterators({&buf01, &buf02}, {1, 0});

    PhraseQueryProcessor qp(&iterators);
    auto positions = qp.Process();

    REQUIRE(positions == Positions{});
  }

  SECTION("Multiple matches") {
    VarintBuffer buf01 = create_varint_buffer({10, 20,     100, 1000});
    VarintBuffer buf02 = create_varint_buffer({11, 21, 88, 101});
    auto iterators = create_iterators({&buf01, &buf02}, {4, 4});

    PhraseQueryProcessor qp(&iterators);
    auto positions = qp.Process();

    REQUIRE(positions == Positions{10, 20, 100});
  }
}



