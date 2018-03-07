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



