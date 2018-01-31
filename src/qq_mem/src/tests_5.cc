#include "catch.hpp"

#include "utils.h"
#include "compression.h"
#include "posting.h"
#include "posting_list_delta.h"
#include "posting_list_vec.h"
#include "intersect.h"

#include "test_helpers.h"


typedef StandardPosting PostingWO;

TEST_CASE( "QueryProcessorDelta works", "[engine0]" ) {
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

  // Setting doc length this will give doc 4 the highest score 
  // because the document is the shortest.
  DocLengthStore store;
  for (int i = 0; i < 5; i++) {
    store.AddLength(i, (5 - i) * 10);
  }

  SECTION("Find top 5") {
    PostingListIterators iterators{pl01.Begin(), pl02.Begin()};

    QueryProcessorDelta processor(iterators, store, 100, 5);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 5);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3, 2, 1, 0});
  }

  SECTION("Find top 2") {
    PostingListIterators iterators{pl01.Begin(), pl02.Begin()};

    QueryProcessorDelta processor(iterators, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 within one postinglist") {
    PostingListIterators iterators{pl01.Begin()};

    QueryProcessorDelta processor(iterators, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 with three posting lists") {
    PostingListIterators iterators{pl01.Begin(), pl02.Begin(), pl03.Begin()};

    QueryProcessorDelta processor(iterators, store, 100, 2);
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

  PostingListStandardVec::PostingListVecIterator it = pl.Begin();

  for (int i = 0; i < 10; i++, it.Advance()) {
    REQUIRE(it.IsEnd() == false);
    REQUIRE(it.DocId() == i);
    REQUIRE(it.TermFreq() == i * 2);
  }
  REQUIRE(it.IsEnd() == true);


}





