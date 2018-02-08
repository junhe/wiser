#include "catch.hpp"

#include "utils.h"
#include "compression.h"
#include "posting.h"
#include "posting_list_delta.h"
#include "posting_list_vec.h"
#include "intersect.h"
#include "qq_mem_engine.h"

#include "test_helpers.h"


TEST_CASE( "Integrated Test for Phrase Query", "[engine0]" ) {
  auto engine = CreateSearchEngine("qq_mem_compressed");
  engine->AddDocument(DocInfo("my hello world program.",
                               // 01234567890123456789012
                              "hello world program",
                              "3,7;.9,13;.15,21;.",
                              "2;.3;.4;."));
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


