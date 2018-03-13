#include "catch.hpp"

#include "utils.h"
#include "compression.h"
#include "posting.h"
#include "posting_list_delta.h"
#include "query_processing.h"
#include "qq_mem_engine.h"

#include "test_helpers.h"

typedef std::vector<std::unique_ptr<PopIteratorService>> PositionIterators;


TEST_CASE( "QueryProcessor2 works", "[engine]" ) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < 10; i++) {
    offset_pairs.push_back(std::make_tuple(i, i)); 
  }

  PostingListDelta pl01("hello");
  PostingListDelta pl02("world");
  PostingListDelta pl03("again");

  for (int i = 0; i < 5; i++) {
    pl01.AddPosting(StandardPosting(i, 3, offset_pairs, {1, 5, 11, 19}));
    pl02.AddPosting(StandardPosting(i, 3, offset_pairs, {2, 8,     20}));
    pl03.AddPosting(StandardPosting(i, 3, offset_pairs, {7, 10}));
  }

  DocLengthStore store;
  for (int i = 0; i < 5; i++) {
    store.AddLength(i, (5 - i) * 10);
  }

  Bm25Similarity similarity(store.GetAvgLength());

  SECTION("Find top 5") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());
    iterators.push_back(pl02.Begin2());

    QueryProcessor2 processor(similarity, &iterators, store, 100, 5, false);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 5);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3, 2, 1, 0});
  }

  SECTION("Do phrase query of 'hello world'") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());
    iterators.push_back(pl02.Begin2());

    QueryProcessor2 processor(similarity, &iterators, store, 100, 5, true);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 5);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    // Table:
    // 1 (0)    19 (3)
    // 2 (0)    20 (2)
    auto &pos_table = result[0].position_table;
    REQUIRE(pos_table.NumUsedRows() == 2);
    REQUIRE(pos_table[0].UsedSize() == 2);
    REQUIRE(pos_table[0][0].pos == 1);
    REQUIRE(pos_table[0][0].term_appearance == 0);
    REQUIRE(pos_table[0][1].pos == 19);
    REQUIRE(pos_table[0][1].term_appearance == 3);

    REQUIRE(pos_table[1].UsedSize() == 2);
    REQUIRE(pos_table[1][0].pos == 2);
    REQUIRE(pos_table[1][0].term_appearance == 0);
    REQUIRE(pos_table[1][1].pos == 20);
    REQUIRE(pos_table[1][1].term_appearance == 2);
    
    // Table
    // 0,0 3,3
    // 0,0 2,2
    std::vector<OffsetPairs> table = result[0].FilterOffsetByPosition();
    REQUIRE(table.size() == 2);
    REQUIRE(table[0].size() == 2);
    REQUIRE(std::get<0>(table[0][0]) == 0);
    REQUIRE(std::get<1>(table[0][0]) == 0);
    REQUIRE(std::get<0>(table[0][1]) == 3);
    REQUIRE(std::get<1>(table[0][1]) == 3);

    REQUIRE(table[1].size() == 2);
    REQUIRE(std::get<0>(table[1][0]) == 0);
    REQUIRE(std::get<1>(table[1][0]) == 0);
    REQUIRE(std::get<0>(table[1][1]) == 2);
    REQUIRE(std::get<1>(table[1][1]) == 2);
    
    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3, 2, 1, 0});
  }

  SECTION("Do phrase query of 'world again'") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl02.Begin2());
    iterators.push_back(pl03.Begin2());

    QueryProcessor2 processor(similarity, &iterators, store, 100, 5, true);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 0);
  }

  SECTION("Find top 2") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());
    iterators.push_back(pl02.Begin2());

    QueryProcessor2 processor(similarity, &iterators, store, 100, 2, false);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 within one postinglist") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());

    QueryProcessor2 processor(similarity, &iterators, store, 100, 2, false);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 with three posting lists") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());
    iterators.push_back(pl02.Begin2());
    iterators.push_back(pl03.Begin2());

    QueryProcessor2 processor(similarity, &iterators, store, 100, 2, false);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }
}


// Note that SingleTermQueryProcessor2 may do lossy/nonlossy calculation
// which may change the results.
TEST_CASE( "SingleTermQueryProcessor2 works", "[engine]" ) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < 10; i++) {
    offset_pairs.push_back(std::make_tuple(i, i)); 
  }

  PostingListDelta pl01("hello");

  for (int i = 0; i < 5; i++) {
    pl01.AddPosting(StandardPosting(i, 3, offset_pairs, {1, 5, 11, 19}));
  }

  // larger doc id -> shorter length -> higher score
  DocLengthStore store;
  for (int i = 0; i < 5; i++) {
    store.AddLength(i, (5 - i) * 10);
  }

  Bm25Similarity similarity(store.GetAvgLength());

  SECTION("Find top 5") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());

    SingleTermQueryProcessor2 processor(similarity, &iterators, store, 100, 5);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 5);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3, 2, 1, 0});
  }

  SECTION("Find top 2") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());

    SingleTermQueryProcessor2 processor(similarity, &iterators, store, 100, 2);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }
}


TEST_CASE( "TwoTermNonPhraseQueryProcessor2 works", "[engine]" ) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < 10; i++) {
    offset_pairs.push_back(std::make_tuple(i, i)); 
  }

  PostingListDelta pl01("hello");
  PostingListDelta pl02("world");
  PostingListDelta pl03("again");

  for (int i = 0; i < 5; i++) {
    pl01.AddPosting(StandardPosting(i, 3, offset_pairs, {1, 5, 11, 19}));
    pl02.AddPosting(StandardPosting(i, 3, offset_pairs, {2, 8,     20}));
    pl03.AddPosting(StandardPosting(i, 3, offset_pairs, {7, 10}));
  }

  DocLengthStore store;
  for (int i = 0; i < 5; i++) {
    store.AddLength(i, (5 - i) * 10);
  }

  Bm25Similarity similarity(store.GetAvgLength());

  SECTION("Find top 5") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());
    iterators.push_back(pl02.Begin2());

    TwoTermNonPhraseQueryProcessor2 processor(similarity, &iterators, store, 100, 5);
    std::vector<ResultDocEntry2> result = processor.Process();
    REQUIRE(result.size() == 5);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3, 2, 1, 0});
  }

  SECTION("Find top 2") {
    std::vector<PostingListDeltaIterator> iterators;
    iterators.push_back(pl01.Begin2());
    iterators.push_back(pl02.Begin2());

    TwoTermNonPhraseQueryProcessor2 processor(similarity, &iterators, store, 100, 2);
    std::vector<ResultDocEntry2> result = processor.Process();
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



TEST_CASE( "PostingListDelta iterator", "[posting_list]" ) {
  auto pl = create_posting_list_delta(10); 

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

  SECTION("Test position iterator") {
    auto it = pl.Begin();
    std::unique_ptr<PopIteratorService> pos_it = it->PositionBegin();
    for (int i = 0; i < 6; i++) {
      REQUIRE(pos_it->IsEnd() == false);
      Position pos = pos_it->Pop();
      REQUIRE(pos == i);
    }
    REQUIRE(pos_it->IsEnd() == true);
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

  SECTION("End offset bound") {
    VarintIteratorEndBound it(buf);

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

PositionIterators create_iterators(std::vector<VarintBuffer *> buffers, std::vector<int> sizes) {
  PositionIterators iterators;
  for (int i = 0; i < buffers.size(); i++) {
    iterators.emplace_back(new VarintIterator(*buffers[i], sizes[i]));
  }
  return iterators;
}

void AssignIterators(PhraseQueryProcessor<VarintIterator> &pqp, 
                     std::vector<VarintBuffer *> buffers, 
                     std::vector<int> sizes) {
  std::vector<VarintIterator> &iterators = *pqp.Iterators();
  for (int i = 0; i < buffers.size(); i++) {
    iterators[i] =  VarintIterator(*buffers[i], sizes[i]);
  }
  pqp.SetNumTerms(buffers.size());
}

void AssignIterators(PhraseQueryProcessor2<VarintIterator> &pqp, 
                     std::vector<VarintBuffer *> buffers, 
                     std::vector<int> sizes) {
  std::vector<VarintIterator> &iterators = *pqp.Iterators();
  for (int i = 0; i < buffers.size(); i++) {
    iterators[i] =  VarintIterator(*buffers[i], sizes[i]);
  }
  pqp.SetNumTerms(buffers.size());
}


TEST_CASE( "PhraseQueryProcessor2", "[engine00]" ) {
  SECTION("Simple") {
    // 3, 4 is a match
    VarintBuffer buf01 = create_varint_buffer(std::vector<uint32_t>{1, 3, 5});
    VarintBuffer buf02 = create_varint_buffer(std::vector<uint32_t>{4});
    
    PhraseQueryProcessor2<VarintIterator> qp(2);
    AssignIterators(qp, {&buf01, &buf02}, {3, 1});
   
    SECTION("Returning info table") {
      auto info_table = qp.Process();
      // REQUIRE(info_table[0][0].pos == 3);
      // REQUIRE(info_table[0][0].term_appearance == 1);

      // REQUIRE(info_table[1][0].pos == 4);
      // REQUIRE(info_table[1][0].term_appearance == 0);
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

    PhraseQueryProcessor2<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {0, 0});

    auto table = qp.Process();
    REQUIRE(table.NumUsedRows() == 0);
    REQUIRE(table[0].UsedSize() == 0);
    REQUIRE(table[1].UsedSize() == 0);
  }

  SECTION("No matches") {
    VarintBuffer buf01 = create_varint_buffer({1, 8, 20});
    VarintBuffer buf02 = create_varint_buffer({0, 7, 19});

    PhraseQueryProcessor2<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {3, 3});

    auto table = qp.Process();

    REQUIRE(table.NumUsedRows() == 0);
  }
 
  SECTION("Bad positions") {
    // Two terms cannot be at the same position
    VarintBuffer buf01 = create_varint_buffer({0});
    VarintBuffer buf02 = create_varint_buffer({0});

    PhraseQueryProcessor2<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {1, 1});

    auto table = qp.Process();

    REQUIRE(table.NumUsedRows() == 0);
  }
 
  SECTION("One list is empty") {
    VarintBuffer buf01 = create_varint_buffer({10});
    VarintBuffer buf02 = create_varint_buffer({});

    PhraseQueryProcessor2<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {1, 0});

    auto table = qp.Process();

    REQUIRE(table.NumUsedRows() == 0);
  }

  SECTION("Multiple matches") {
    VarintBuffer buf01 = create_varint_buffer({10, 20,     100, 1000});
    VarintBuffer buf02 = create_varint_buffer({11, 21, 88, 101});

    PhraseQueryProcessor2<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {4, 4});

    auto table = qp.Process();

    REQUIRE(table.NumUsedRows() == 2);
    REQUIRE(table[0].UsedSize() == 3);
    REQUIRE(table[0][0].pos == 10);
    REQUIRE(table[0][1].pos == 20);
    REQUIRE(table[0][2].pos == 100);

    REQUIRE(table[1].UsedSize() == 3);
    REQUIRE(table[1][0].pos == 11);
    REQUIRE(table[1][1].pos == 21);
    REQUIRE(table[1][2].pos == 101);
  }
}


TEST_CASE( "PhraseQueryProcessor", "[engine00]" ) {
  SECTION("Simple") {
    // 3, 4 is a match
    VarintBuffer buf01 = create_varint_buffer(std::vector<uint32_t>{1, 3, 5});
    VarintBuffer buf02 = create_varint_buffer(std::vector<uint32_t>{4});
    
    PhraseQueryProcessor<VarintIterator> qp(2);
    AssignIterators(qp, {&buf01, &buf02}, {3, 1});
   
    SECTION("Returning info table") {
      auto info_table = qp.Process();
      // REQUIRE(info_table[0][0].pos == 3);
      // REQUIRE(info_table[0][0].term_appearance == 1);

      // REQUIRE(info_table[1][0].pos == 4);
      // REQUIRE(info_table[1][0].term_appearance == 0);
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

    PhraseQueryProcessor<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {0, 0});

    auto table = qp.Process();
    REQUIRE(table.size() == 2); // two rows, each for a term
    REQUIRE(table[0].size() == 0);
    REQUIRE(table[1].size() == 0);
  }

  SECTION("No matches") {
    VarintBuffer buf01 = create_varint_buffer({1, 8, 20});
    VarintBuffer buf02 = create_varint_buffer({0, 7, 19});

    PhraseQueryProcessor<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {3, 3});

    auto table = qp.Process();

    REQUIRE(table[0].size() == 0);
  }
 
  SECTION("Bad positions") {
    // Two terms cannot be at the same position
    VarintBuffer buf01 = create_varint_buffer({0});
    VarintBuffer buf02 = create_varint_buffer({0});

    PhraseQueryProcessor<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {1, 1});

    auto table = qp.Process();

    REQUIRE(table[0].size() == 0);
  }
 
  SECTION("One list is empty") {
    VarintBuffer buf01 = create_varint_buffer({10});
    VarintBuffer buf02 = create_varint_buffer({});

    PhraseQueryProcessor<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {1, 0});

    auto table = qp.Process();

    REQUIRE(table[0].size() == 0);
  }

  SECTION("Multiple matches") {
    VarintBuffer buf01 = create_varint_buffer({10, 20,     100, 1000});
    VarintBuffer buf02 = create_varint_buffer({11, 21, 88, 101});

    PhraseQueryProcessor<VarintIterator> qp(10);
    AssignIterators(qp, {&buf01, &buf02}, {4, 4});

    auto table = qp.Process();

    REQUIRE(table.size() == 2);
    REQUIRE(table[0].size() == 3);
    REQUIRE(table[0][0].pos == 10);
    REQUIRE(table[0][1].pos == 20);
    REQUIRE(table[0][2].pos == 100);

    REQUIRE(table[1].size() == 3);
    REQUIRE(table[1][0].pos == 11);
    REQUIRE(table[1][1].pos == 21);
    REQUIRE(table[1][2].pos == 101);

    // REQUIRE(positions == Positions{10, 20, 100});
  }
}


TEST_CASE( "DocInfo", "[engine]" ) {
  // hello, this is the hello         hello  this      0-4,19-23;7-10;.    1;5;.2;.
  DocInfo doc_info("hello, this is the hello", "hello this", 
               "0,4;19,23.7-10;.", "1;5;.2;.", "WITH_POSITIONS");

  SECTION("Position parsing") {
    auto pos_table = doc_info.GetPositions();
    REQUIRE(pos_table.size() == 2);
    REQUIRE(pos_table[0] == Positions{1, 5});
    REQUIRE(pos_table[1] == Positions{2});
  }

  SECTION("Load line docs with positions") {
    auto engine = CreateSearchEngine("qq_mem_compressed");
    engine->AddDocument(doc_info);
    REQUIRE(engine->TermCount() == 2);
  }

  SECTION("Load documents to inverted index") {
    InvertedIndexQqMemDelta index;
    index.AddDocument(0, doc_info);
    auto size_map = index.PostinglistSizes({"hello"});
    REQUIRE(size_map["hello"] == 1);
    REQUIRE(index.Size() == 2);
  }
}


TEST_CASE( "Loading Engine From Local Line Doc", "[engine]" ) {
  auto engine = CreateSearchEngine("qq_mem_compressed");
  REQUIRE(engine->TermCount() == 0);
  engine->LoadLocalDocuments("src/testdata/line_doc_with_positions", 10000, 
      "WITH_POSITIONS");
  REQUIRE(engine->TermCount() > 0);
  auto result = engine->Search(SearchQuery({"prefix"}));
  std::cout << result.ToStr() << std::endl;
  REQUIRE(result.Size() > 0);

  {
    SearchQuery query({"solar", "radiation"}, true);
    result = engine->Search(query);
    std::cout << result.ToStr() << std::endl;
    REQUIRE(result.Size() > 0);
  }
}


