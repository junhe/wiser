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



