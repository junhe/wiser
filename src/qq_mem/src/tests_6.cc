#include "catch.hpp"

#include <memory>

#include "utils.h"
#include "compression.h"
#include "posting.h"
#include "posting_list_delta.h"
#include "query_processing.h"
#include "qq_mem_engine.h"
#include "query_pool.h"

#include "test_helpers.h"


TEST_CASE( "Integrated Test for Phrase Query", "[engine0]" ) {
  auto engine = CreateSearchEngine("qq_mem_compressed");
  engine->AddDocument(DocInfo("my hello world program.",
                               // 01234567890123456789012
                              "hello world program",
                              "3,7;.9,13;.15,21;.",
                              "2;.3;.4;.", "WITH_POSITIONS"));
  REQUIRE(engine->TermCount() == 3);

  SECTION("Sanity") {
    auto result = engine->Search(SearchQuery({"hello"}, true));
    REQUIRE(result.Size() == 1);
  }

  SECTION("Intersection") {
    auto result = engine->Search(SearchQuery({"hello", "world"}, true));
    REQUIRE(result.Size() == 1);
  }
  
  SECTION("Phrase") {
    SECTION("hello world") {
      SearchQuery query({"hello", "world"}, true);
      query.is_phrase = true;
      auto result = engine->Search(query);
      REQUIRE(result.Size() == 1);
    }

    SECTION("non-existing") {
      SearchQuery query({"world", "hello"}, true);
      query.is_phrase = true;
      auto result = engine->Search(query);
      REQUIRE(result.Size() == 0);
    }
  }
}



TEST_CASE( "Encode offset pairs", "[posting]" ) {
  SECTION("Empty") {
    StandardPosting posting(0, 0, OffsetPairs{});
    VarintBuffer buf = posting.EncodeOffsets();

    SECTION("Using varint iterator") {
      VarintIteratorEndBound it(buf);

      REQUIRE(it.IsEnd() == true);
    }

    SECTION("Using compressed data iterator") {
      CompressedPairIterator it(buf.DataPointer(), 0, buf.Size());    

      REQUIRE(it.IsEnd() == true);
    }
  }

 
  SECTION("Regular") {
    StandardPosting posting(0, 0, 
        OffsetPairs{OffsetPair{8, 9}, OffsetPair{20, 22}});
    VarintBuffer buf = posting.EncodeOffsets();

    SECTION("Using varint iterator") {
      VarintIteratorEndBound it(buf);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 8);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 1);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 11);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 2);

      REQUIRE(it.IsEnd() == true);
    }

    SECTION("Using compressed data iterator") {
      CompressedPairIterator it(buf.DataPointer(), 0, buf.Size());    
      OffsetPair pair;

      REQUIRE(it.IsEnd() == false);
      it.Pop(&pair);
      REQUIRE(pair == OffsetPair{8, 9});

      REQUIRE(it.IsEnd() == false);
      it.Pop(&pair);
      REQUIRE(pair == OffsetPair{20, 22});

      REQUIRE(it.IsEnd() == true);
    }
  }
}

TEST_CASE( "Encode positions", "[posting]" ) {
  SECTION("Empty") {
    StandardPosting posting(0, 0);
    VarintBuffer buf = posting.EncodePositions();

    SECTION("Using Varint Iterator") {
      VarintIteratorEndBound it(buf);
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("Using position iterator") {
      CompressedPositionIterator it(buf.DataPointer(), 0, buf.Size());
      REQUIRE(it.IsEnd() == true);
    }
  }
 
  SECTION("Regular") {
    StandardPosting posting(0, 0, 
        OffsetPairs{}, Positions{10, 21, 25});
    VarintBuffer buf = posting.EncodePositions();

    SECTION("Using Varint Iterator") {
      VarintIteratorEndBound it(buf);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 10);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 11);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 4);

      REQUIRE(it.IsEnd() == true);
    }

    SECTION("Using position iterator") {
      CompressedPositionIterator it(buf.DataPointer(), 0, buf.Size());

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 10);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 21);

      REQUIRE(it.IsEnd() == false);
      REQUIRE(it.Pop() == 25);

      REQUIRE(it.IsEnd() == true);
    }
  }
}

// dataset must have even number of items
PositionInfoArray CreateInfoArr(std::vector<int> dataset) {
  PositionInfoArray info_arr(dataset.size() / 2);
  for (int i = 0; i < dataset.size(); i += 2) {
    //              pos         term_appearance
    info_arr.Append(dataset[i], dataset[i+1]);
  }
  return info_arr;
}

// table must be a rectangle table
PositionInfoTable2 CreateInfoTable(std::vector< std::vector<std::pair<int, int>> > table) {
  PositionInfoTable2 pos_table(table.size(), table[0].size());

  for (int row = 0; row < table.size(); row++) {
    for (int col = 0; col < table[row].size(); col++) {
      pos_table.Append(row, table[row][col].first, table[row][col].second); 
    }
  }
  return pos_table;
}


PositionInfoVec CreateInfoVec(std::vector<int> dataset) {
  PositionInfoVec info_vec;
  for (int i = 0; i < dataset.size(); i += 2) {
    PositionInfo info;
    info.pos = dataset[i];
    info.term_appearance = dataset[i+1];
    info_vec.push_back(info);
  }
  return info_vec;
}

VarintBuffer CreateOffsetPairBuf(std::vector<int> dataset) {
  VarintBuffer buf;
  for (auto m : dataset) {
    buf.Append(m);
  }
  return buf;
}

std::shared_ptr<OffsetPairsIteratorService> CreateOffsetIter(VarintBuffer *buf) {
  std::shared_ptr<OffsetPairsIteratorService> iter(new CompressedPairIterator(buf->DataPointer(), 0, buf->Size()));
  return iter;
}

TEST_CASE( "Filter offsets by positions in ResultDocEntry2", "[result]" ) {

  PositionInfoTable2 position_table = CreateInfoTable(
      {
        { {0, 2}, {0, 4} },
        { {0, 1}, {0, 2} }
      }
  );

  //                                        0, 1,   3, 6,   10,15   21,28,  36,45
  VarintBuffer buf_1 = CreateOffsetPairBuf({0, 1,   2, 3,   4, 5,   6, 7,   8, 9});
  auto iter_1 = CreateOffsetIter(&buf_1);
  //                                        10, 21,   33, 46,  60, 75
  VarintBuffer buf_2 = CreateOffsetPairBuf({10, 11,   12, 13,  14, 15});
  auto iter_2 = CreateOffsetIter(&buf_2);
  OffsetIterators offset_iters{iter_1, iter_2};
  
  ResultDocEntry2 entry(0, 1.0, offset_iters, position_table, true);
  std::vector<OffsetPairs> ret = entry.FilterOffsetByPosition();

  REQUIRE(ret.size() == 2);
  // first term
  REQUIRE(std::get<0>(ret[0][0]) == 10);
  REQUIRE(std::get<1>(ret[0][0]) == 15);

  REQUIRE(std::get<0>(ret[0][1]) == 36);
  REQUIRE(std::get<1>(ret[0][1]) == 45);
    
  // second term
  REQUIRE(std::get<0>(ret[1][0]) == 33);
  REQUIRE(std::get<1>(ret[1][0]) == 46);

  REQUIRE(std::get<0>(ret[1][1]) == 60);
  REQUIRE(std::get<1>(ret[1][1]) == 75);
}


TEST_CASE( "Filter offsets by positions", "[result]" ) {
  PositionInfoTable position_table{
    CreateInfoVec({0, 2,   0, 4}),
    CreateInfoVec({0, 1,   0, 2})
  };

  //                                        0, 1,   3, 6,   10,15   21,28,  36,45
  VarintBuffer buf_1 = CreateOffsetPairBuf({0, 1,   2, 3,   4, 5,   6, 7,   8, 9});
  auto iter_1 = CreateOffsetIter(&buf_1);
  //                                        10, 21,   33, 46,  60, 75
  VarintBuffer buf_2 = CreateOffsetPairBuf({10, 11,   12, 13,  14, 15});
  auto iter_2 = CreateOffsetIter(&buf_2);
  OffsetIterators offset_iters{iter_1, iter_2};
  
  ResultDocEntry entry(0, 1.0, offset_iters, position_table, true);
  std::vector<OffsetPairs> ret = entry.FilterOffsetByPosition();

  REQUIRE(ret.size() == 2);
  // first term
  REQUIRE(std::get<0>(ret[0][0]) == 10);
  REQUIRE(std::get<1>(ret[0][0]) == 15);

  REQUIRE(std::get<0>(ret[0][1]) == 36);
  REQUIRE(std::get<1>(ret[0][1]) == 45);
    
  // second term
  REQUIRE(std::get<0>(ret[1][0]) == 33);
  REQUIRE(std::get<1>(ret[1][0]) == 46);

  REQUIRE(std::get<0>(ret[1][1]) == 60);
  REQUIRE(std::get<1>(ret[1][1]) == 75);
}

TEST_CASE( "Return offset vector", "[result]" ) {
  //                                        0, 1,   3, 6   
  VarintBuffer buf_1 = CreateOffsetPairBuf({0, 1,   2, 3});
  auto iter_1 = CreateOffsetIter(&buf_1);
  //                                        10, 21,   33, 46,  60, 75
  VarintBuffer buf_2 = CreateOffsetPairBuf({10, 11,   12, 13,  14, 15});
  auto iter_2 = CreateOffsetIter(&buf_2);
  OffsetIterators offset_iters{iter_1, iter_2};

  ResultDocEntry entry(0, 1.0);
  entry.offset_iters = offset_iters;

  std::vector<OffsetPairs> table = entry.ExpandOffsets();
  REQUIRE(table.size() == 2);
  REQUIRE(table[0].size() == 2);
  REQUIRE(std::get<0>(table[0][0]) == 0);
  REQUIRE(std::get<1>(table[0][0]) == 1);
  REQUIRE(std::get<0>(table[0][1]) == 3);
  REQUIRE(std::get<1>(table[0][1]) == 6);

  REQUIRE(table[1].size() == 3);
  REQUIRE(std::get<0>(table[1][0]) == 10);
  REQUIRE(std::get<1>(table[1][0]) == 21);
  REQUIRE(std::get<0>(table[1][1]) == 33);
  REQUIRE(std::get<1>(table[1][1]) == 46);
  REQUIRE(std::get<0>(table[1][2]) == 60);
  REQUIRE(std::get<1>(table[1][2]) == 75);
}


TEST_CASE( "QueryProducer", "[query]" ) {
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(2));
  array->LoadTerms({"hello"});

  GeneralConfig config;
  config.SetInt("n_results", 8);
  QueryProducer producer(std::move(array), config);
  REQUIRE(producer.Size() == 2);

  SearchQuery query;
  query = producer.NextNativeQuery(0);
  REQUIRE(query.n_results == 8);
  REQUIRE(query.terms == TermList{"hello"});

  query = producer.NextNativeQuery(0);
  REQUIRE(query.n_results == 8);
  REQUIRE(query.terms == TermList{"hello"});


  query = producer.NextNativeQuery(1);
  REQUIRE(query.n_results == 8);
  REQUIRE(query.terms == TermList{"hello"});

  query = producer.NextNativeQuery(1);
  REQUIRE(query.n_results == 8);
  REQUIRE(query.terms == TermList{"hello"});
}







