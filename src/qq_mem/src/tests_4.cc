#include "catch.hpp"

#include "utils.h"
#include "compression.h"
#include "posting.h"
#include "posting_list_delta.h"



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
  SECTION("No offsets") {
    RankingPostingWithOffsets posting(3, 4); 
    std::string buf = posting.Encode();
    REQUIRE(buf[0] == 2); // num of bytes starting from doc id
    REQUIRE(buf[1] == 3); // doc id
    REQUIRE(buf[2] == 4); //  TF
  }

  SECTION("With offset pairs") {
    OffsetPairs offset_pairs;
    for (int i = 0; i < 10; i++) {
      offset_pairs.push_back(std::make_tuple(1, 2)); 
    }

    RankingPostingWithOffsets posting(3, 4, offset_pairs); 
    std::string buf = posting.Encode();
    REQUIRE(buf[0] == 22); // content size: 2 + 2 * 10 = 22
    REQUIRE(buf[1] == 3); // doc id
    REQUIRE(buf[2] == 4); //  TF

    const auto PRE = 3; // size | doc_id | TF | 
    for (int i = 0; i < 10; i++) {
      REQUIRE(buf[PRE + 2 * i] == 1);
      REQUIRE(buf[PRE + 2 * i + 1] == 2);
    }
  }
}


TEST_CASE( "Posting List Delta", "[postinglist]" ) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < 10; i++) {
    offset_pairs.push_back(std::make_tuple(1, 2)); 
  }

  RankingPostingWithOffsets posting(3, 4, offset_pairs); 

  SECTION("Add posting") {
    PostingListDelta pl("hello");
    REQUIRE(pl.Size() == 0);

    pl.AddPosting(posting);

    REQUIRE(pl.Size() == 1);
    REQUIRE(pl.ByteCount() == posting.Encode().size());

    pl.AddPosting(posting);

    REQUIRE(pl.Size() == 2);
    REQUIRE(pl.ByteCount() == posting.Encode().size() * 2);
  }

}



