#include "catch.hpp"

#include <memory>

#include "utils.h"
#include "compression.h"
#include "qq_mem_engine.h"
#include "doc_store.h"
#include "doc_length_store.h"

#include "test_helpers.h"


TEST_CASE( "Minimum decoding", "[pl_delta]" ) {
  PostingListDelta pl = create_posting_list_delta(302);
  auto it = pl.Begin();

  SECTION("Old advance") {
    int i = 0;
    while (it->IsEnd() == false) {
      REQUIRE(it->DocId() == i);
      REQUIRE(it->TermFreq() == i * 2);
      i++;
      it->Advance();
    }
  }

  SECTION("Advance only") {
    int i = 0;
    while (it->IsEnd() == false) {
      REQUIRE(it->DocId() == i);
      REQUIRE(it->TermFreq() == i * 2);
      i++;
      it->AdvanceOnly();
      it->DecodeContSizeAndDocId();
      it->DecodeTf();
      it->DecodeOffsetSize();
    }
  }

  SECTION("SkipForward") {
    int i = 0;
   
    while (it->IsEnd() == false) {
      REQUIRE(it->DocId() == i);
      REQUIRE(it->TermFreq() == i * 2);
      i++;
      it->SkipForward_MinDecode(i);
      it->DecodeTf();
    }
  }

  SECTION("SkipForward a lot") {
    REQUIRE(it->DocId() == 0);
    REQUIRE(it->TermFreq() == 0);

    it->SkipForward_MinDecode(150);
    it->DecodeTf();
    REQUIRE(it->DocId() == 150);
    REQUIRE(it->TermFreq() == 300);

    it->SkipForward_MinDecode(200);
    it->DecodeTf();
    REQUIRE(it->DocId() == 200);
    REQUIRE(it->TermFreq() == 400);
  }
}

TEST_CASE( "Compress int", "[utils]" ) {
  SECTION("Finding number of bits") {
    REQUIRE(utils::NumOfBits(0) == 0); 
    REQUIRE(utils::NumOfBits(1) == 1); 
    REQUIRE(utils::NumOfBits(8) == 4); 
    REQUIRE(utils::NumOfBits(12) == 4); 
    REQUIRE(utils::NumOfBits(0xff) == 8); 
    REQUIRE(utils::NumOfBits(0x7f) == 7); 
    REQUIRE(utils::NumOfBits(0x7fffffff) == 31); 
    REQUIRE(utils::NumOfBits(0xffffffff) == 32); 
  }

  SECTION("Compress int to char") {
    REQUIRE(utils::UintToChar4(0) == 0); 
    REQUIRE(utils::UintToChar4(1) == 1); 
    REQUIRE(utils::UintToChar4(7) == 7); 
    REQUIRE(utils::UintToChar4(8) == 0x08); 
    REQUIRE(utils::UintToChar4(0x80) == 0x28); 
    REQUIRE(utils::UintToChar4(0xffffffff) == (char(((29 << 3) | 0x07) & 0xff))); 
  }

  SECTION("Decompress char to int") {
    REQUIRE(utils::Char4ToUint(0) == 0);
    REQUIRE(utils::Char4ToUint(1) == 1);
    REQUIRE(utils::Char4ToUint(7) == 7);
    REQUIRE(utils::Char4ToUint(8) == 8);
    REQUIRE(utils::Char4ToUint(0x28) == 0x80); 
    REQUIRE(utils::Char4ToUint(char(((29 << 3) | 0x07) & 0xff)) == uint32_t(0xf0000000)); 
  }

  SECTION("Shift is larger than 28") {
    // We keep 4 left-most bits. If you shift 29, you will lost one bit
    // In this followingn case, you will get 0.
    REQUIRE(utils::Char4ToUint(240) == 0); 
    REQUIRE(utils::Char4ToUint(248) == 0); 
    REQUIRE(utils::Char4ToUint(252) == 0); 
  }

  SECTION("Compress and decompress ") {
    for (uint32_t i = 0; i < 0x0fffffff; i += 777777) {
      char encoded = utils::UintToChar4(i); 
      uint32_t decoded = utils::Char4ToUint(encoded);
      int n = utils::NumOfBits(decoded);
      int shift = n - 4;

      REQUIRE(utils::NumOfBits(decoded) == utils::NumOfBits(i));
      REQUIRE((decoded >> shift) == (i >> shift));
    }
  }
}


TEST_CASE( "Scoring as ElasticSearch does", "[scoring]" ) {
  SECTION("ElasticSearch IDF") {
    REQUIRE(utils::format_double(calc_es_idf(1, 1), 3) == "0.288"); // From an ES run
    REQUIRE(utils::format_double(calc_es_idf(3, 1), 3)== "0.981"); // From an ES run
  }

  SECTION("ElasticSearch TF NORM") {
    REQUIRE(calc_es_tfnorm(1, 3, 3.0) == 1.0); // From an ES run
    REQUIRE(calc_es_tfnorm(1, 7, 7.0) == 1.0); // From an ES run
    REQUIRE( utils::format_double(calc_es_tfnorm(1, 2, 8/3.0), 3) == "1.11"); // From an ES run
  }
}


TEST_CASE( "Scoring (Bm25Similarity) as ElasticSearch does", "[scoring]" ) {
  SECTION("Bm25Similarity Idf") {
    REQUIRE(utils::format_double(Bm25Similarity::Idf(1, 1), 3) == "0.288"); // From an ES run
    REQUIRE(utils::format_double(Bm25Similarity::Idf(3, 1), 3)== "0.981"); // From an ES run
  }

  SECTION("ElasticSearch TF NORM with Bm25Similarity") {
    REQUIRE(Bm25Similarity::TfNormStatic(1, 3, 3.0) == 1.0);
    REQUIRE(Bm25Similarity::TfNormStatic(1, 7, 7.0) == 1.0);
    REQUIRE(utils::format_double(Bm25Similarity::TfNormStatic(1, 2, 8/3.0), 3) == "1.11");
  }

  SECTION("Bm25Similarity Lossy TfNorm") {
    // For field length 3, 7, 8, there should be no loss  
    {
      Bm25Similarity sim(3.0);
      REQUIRE(sim.TfNormLossy(1, 3) == 1.0);
    }
    {
      Bm25Similarity sim(7.0);
      REQUIRE(sim.TfNormLossy(1, 7) == 1.0);
    }
    {
      Bm25Similarity sim(8 / 3.0);
      REQUIRE(utils::format_double(sim.TfNormLossy(1, 2), 3) == "1.11");
    }
  }
}






