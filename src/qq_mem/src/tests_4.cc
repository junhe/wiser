#include "catch.hpp"

#include "utils.h"
#include "compression.h"
#include "posting.h"
#include "posting_list_delta.h"

#include "test_helpers.h"



void test_encoding(uint32_t val) {
  std::string buf(8, '\0');
  uint32_t val2;

  auto len1 = utils::varint_encode(val, &buf, 0);
  auto len2 = utils::varint_decode(buf, 0, &val2);
  
  REQUIRE(val == val2);
  REQUIRE(len1 == len2);
  REQUIRE(len1 > 0);
}


void test_expand_and_encode(uint32_t val) {
  std::string buf;
  uint32_t val2;

  auto len1 = utils::varint_expand_and_encode(val, &buf, 0);
  auto len2 = utils::varint_decode(buf, 0, &val2);
  
  REQUIRE(val == val2);
  REQUIRE(len1 == len2);
  REQUIRE(len1 > 0);
}


TEST_CASE( "varint", "[varint]" ) {
  test_encoding(0);
  test_encoding(10);
  test_encoding(300);
  test_encoding(~0x00); // largest number possible

  test_expand_and_encode(0);
  test_expand_and_encode(10);
  test_expand_and_encode(300);
  test_expand_and_encode(~0x00); // largest number possible

  std::string buf(8, '\0');
  uint32_t val02;

  SECTION("Protobuf example") {
    auto len1 = utils::varint_encode(300, &buf, 0);
    REQUIRE(len1 == 2);
    // printf("----------=\n");
    // printf("%04x \n", buf[0] & 0xff);
    // printf("%04x \n", buf[1] & 0xff);
    REQUIRE((buf[0] & 0xff) == 0xAC);
    REQUIRE((buf[1] & 0xff) == 0x02);

    auto len2 = utils::varint_decode(buf, 0, &val02);
    REQUIRE(len2 == 2);
    REQUIRE(val02 == 300);
  }

  SECTION("Protobuf example") {
    auto len1 = utils::varint_encode(0, &buf, 0);
    REQUIRE(len1 == 1);
    // printf("----------=\n");
    // printf("%04x \n", buf[0] & 0xff);
    // printf("%04x \n", buf[1] & 0xff);
    REQUIRE((buf[0] & 0xff) == 0x00);

    auto len2 = utils::varint_decode(buf, 0, &val02);
    REQUIRE(len2 == 1);
    REQUIRE(val02 == 0);
  }
}

TEST_CASE( "VarintBuffer", "[varint]" ) {
  SECTION("Append and prepend number") {
    VarintBuffer buf;
    REQUIRE(buf.End() == 0);

    buf.Append(1);
    REQUIRE(buf.End() == 1);

    auto data = buf.Data();
    REQUIRE(data[0] == 1);

    buf.Prepend(2);
    REQUIRE(buf.End() == 2);

    data = buf.Data();
    REQUIRE(data[0] == 2);

    REQUIRE(data.size() == 2);
  }

  SECTION("Append and prepend string buffer") {
    VarintBuffer buf;
    buf.Append("abc");
    buf.Append("def");
    REQUIRE(buf.Size() == 6);
    REQUIRE(buf.Data() == "abcdef");

    buf.Prepend("01");
    REQUIRE(buf.Size() == 8);
    REQUIRE(buf.Data() == "01abcdef");
  }
}


TEST_CASE( "Encoding posting", "[encoding]" ) {
  SECTION("No offsets and no position") {
    // content_size | doc id delta | freq | offset size |
    //            3 |            3 |    4 |           0 |
    StandardPosting posting(3, 4); 
    std::string buf = posting.Encode();
    REQUIRE(buf[0] == 3); // num of bytes starting from doc id
    REQUIRE(buf[1] == 3); // doc id
    REQUIRE(buf[2] == 4); //  TF
  }

  SECTION("With offset pairs but no positions") {
    OffsetPairs offset_pairs;
    for (int i = 1; i < 11; i++) {
      offset_pairs.push_back(std::make_tuple(i, i)); 
    }

    StandardPosting posting(3, 4, offset_pairs); 
    // content_size | doc id delta | freq | offset size | ... 
    //           23 |            3 |    4 |          20 | ... 10 x 2
    std::string buf = posting.Encode();
    REQUIRE(buf[0] == 23); // content size: 2 + 2 * 10 = 22
    REQUIRE(buf[1] == 3); // doc id
    REQUIRE(buf[2] == 4); //  TF
    REQUIRE(buf[3] == 20); //  offset size

    const auto PRE = 4; // size | doc_id | TF | off size|
    for (int i = 0; i < 10; i++) {
      REQUIRE(buf[PRE + 2 * i] == 1);
      REQUIRE(buf[PRE + 2 * i + 1] == 0);
    }
  }
}

void test_offset_iterator(std::unique_ptr<OffsetPairsIteratorService> offset_it, 
    OffsetPairs original_pairs) {
  OffsetPairs pairs(original_pairs.size());
  for (int i = 0; i < original_pairs.size(); i++) {
    REQUIRE(offset_it->IsEnd() == false);
    offset_it->Pop(&pairs[i]);
    REQUIRE(std::get<0>(pairs[i]) == std::get<0>(original_pairs[i]));
    REQUIRE(std::get<1>(pairs[i]) == std::get<1>(original_pairs[i]));
  }

  // check the end
  {
    REQUIRE(offset_it->IsEnd() == true);
  }
}

void test_if_two_postings_equal(StandardPosting a, StandardPosting b) {
    REQUIRE(a.GetDocId() == b.GetDocId());
    REQUIRE(a.GetTermFreq() == b.GetTermFreq());
    REQUIRE(*a.GetOffsetPairs() == *b.GetOffsetPairs());
}

void test_posting_list_delta( StandardPosting postingA,
    StandardPosting postingB) {
  // Initialize posting list
  PostingListDelta pl("hello");
  REQUIRE(pl.Size() == 0);

  pl.AddPosting(postingA);

  REQUIRE(pl.Size() == 1);
  REQUIRE(pl.ByteCount() == postingA.Encode().size());

  pl.AddPosting(postingB);

  REQUIRE(pl.Size() == 2);
  REQUIRE(pl.ByteCount() == postingA.Encode().size() + postingB.Encode().size() );

  // Iterate
  auto it = pl.NativeBegin();
  REQUIRE(it->IsEnd() == false);
  REQUIRE(it->DocId() == postingA.GetDocId());
  REQUIRE(it->TermFreq() == postingA.GetTermFreq());
  // REQUIRE(it->OffsetPairStart() == 3); // size|id|tf|offset
  // REQUIRE(it->CurContentBytes() == postingA.Encode().size() - 1); // size|id|tf|offset

  // check offsets
  {
    auto offset_it = it->OffsetPairsBegin();
    auto original_pairs = *postingA.GetOffsetPairs();
    test_offset_iterator(std::move(offset_it), original_pairs);
  }

  // Iterate
  it->Advance();
  REQUIRE(it->IsEnd() == false);
  REQUIRE(it->DocId() == postingB.GetDocId());
  REQUIRE(it->TermFreq() == postingB.GetTermFreq());
  // REQUIRE(it->OffsetPairStart() == postingA.Encode().size() + 3); // size|id|tf|offset
  // REQUIRE(it->CurContentBytes() == postingB.Encode().size() - 1); // size|id|tf|offset

  // check offsets
  {
    auto offset_it = it->OffsetPairsBegin();
    auto original_pairs = *postingB.GetOffsetPairs();
    test_offset_iterator(std::move(offset_it), original_pairs);
  }

  it->Advance();
  REQUIRE(it->IsEnd() == true);
}

TEST_CASE( "Posting List Delta", "[postinglist]" ) {
  SECTION("10 postings") {
    OffsetPairs offset_pairsA;
    for (int i = 0; i < 10; i++) {
      offset_pairsA.push_back(std::make_tuple(i * 2, i * 2 + 1)); 
    }

    OffsetPairs offset_pairsB;
    for (int i = 20; i < 23; i++) {
      offset_pairsB.push_back(std::make_tuple(i * 2, i * 2 + 1)); 
    }

    StandardPosting postingA(3, 4, offset_pairsA); 
    StandardPosting postingB(8, 9, offset_pairsB); 

    test_posting_list_delta(postingA, postingB);
  }

  SECTION("0 postings") {
    OffsetPairs offset_pairs;

    StandardPosting postingA(3, 4, offset_pairs); 
    StandardPosting postingB(8, 9, offset_pairs); 

    test_posting_list_delta(postingA, postingB);
  }
}


TEST_CASE( "Skip list", "[postinglist0]" ) {
  PostingListDelta pl("hello", 3);
  for (int i = 0; i < 10; i++) {
    pl.AddPosting(create_posting(i, i + 1, 0));
  }

  SECTION("SkipIndex") {
    auto skip_index = pl.GetSkipIndex();

    REQUIRE(skip_index.size() == 4);

    std::vector<DocIdType> doc_ids;
    std::vector<int> start_offsets;
    for (auto meta : skip_index) {
      doc_ids.push_back(meta.prev_doc_id);
      start_offsets.push_back(meta.start_offset);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{0, 2, 5, 8});
    // each posting takes cont_size | doc id | freq | offset size|
    // Each span is then 4 * 3 = 12
    REQUIRE(start_offsets == std::vector<int>{0, 12, 12*2, 12*3});
  }

  SECTION("Advance") {
    auto it = pl.NativeBegin();
    std::vector<int> has_skip, not_has_skip;
    std::vector<int> span_doc_ids;
    for (int i = 0; i < 10; i++) {
      REQUIRE(it->DocId() == i);
      REQUIRE(it->TermFreq() == i + 1);

      if (it->HasSkip()) {
        has_skip.push_back(i);
        span_doc_ids.push_back(it->NextSpanDocId());
      } else {
        not_has_skip.push_back(i);
      }

      auto ret = it->Advance();
      REQUIRE(ret == true);
    }

    REQUIRE(has_skip == std::vector<int>{0, 3, 6});
    REQUIRE(span_doc_ids == std::vector<int>{2, 5, 8});
    REQUIRE(not_has_skip == std::vector<int>{1, 2, 4, 5, 7, 8, 9});

    auto ret = it->Advance();
    REQUIRE(ret == false);
  }

  SECTION("Skip to next span") {
    auto it = pl.NativeBegin();

    REQUIRE(it->HasSkip() == true); // at posting[0]

    it->SkipToNextSpan();
    REQUIRE(it->DocId() == 3);
    REQUIRE(it->HasSkip() == true); // at posting[3]

    it->SkipToNextSpan();
    REQUIRE(it->DocId() == 6);
    REQUIRE(it->HasSkip() == true); // at posting[6]

    it->SkipToNextSpan();
    REQUIRE(it->DocId() == 9);
    REQUIRE(it->HasSkip() == false); // at posting[9]
  }

  SECTION("Skip forward") {
    auto it = pl.NativeBegin();

    SECTION("One by one") {
      for (int i = 0; i < 10; i++) {
        it->SkipForward(i);
        REQUIRE(it->DocId() == i);
      }
    }

    SECTION("2 by 2") {
      for (int i = 0; i < 10; i += 2) {
        it->SkipForward(i);
        REQUIRE(it->DocId() == i);
      }
    }

    SECTION("Skip to it->elf") {
      it->SkipForward(5);
      REQUIRE(it->DocId() == 5);

      it->SkipForward(1);
      REQUIRE(it->DocId() == 5);
    }

    SECTION("doc_id is before it->rator") {
      it->SkipForward(5);
      REQUIRE(it->DocId() == 5);

      it->SkipForward(1);
      REQUIRE(it->DocId() == 5);
    }

    SECTION("doc_id is beyodn the end") {
      it->SkipForward(5);
      REQUIRE(it->DocId() == 5);

      it->SkipForward(100);
      REQUIRE(it->IsEnd() == true);
    }
  }


}



