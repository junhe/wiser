#include "catch.hpp"

#include <iostream>

#include "test_helpers.h"
#include "bloom_filter.h"
#include "flash_containers.h"


void CheckSkipList(int n) {
  std::vector<off_t> offs;
  for (int i = 0; i < n; i++) {
    offs.push_back(PsudoIncreasingRandom(i));
  }

  BloomSkipListWriter writer(offs);
  std::string data = writer.Serialize();

  REQUIRE(data[0] == (char) BLOOM_SKIP_LIST_FIRST_BYTE);

  BloomSkipList skiplist;
  skiplist.Load((const uint8_t *)data.data());
  REQUIRE(skiplist.Size() == offs.size());

  for (int i = 0; i < n; i++) {
    REQUIRE(skiplist[i] == offs[i]);
  }
}


TEST_CASE( "Bloom box skip list", "[bloomfilter]" ) {
  SECTION("Simple") {
    CheckSkipList(0);
    CheckSkipList(1);
    CheckSkipList(10);
    CheckSkipList(100);
  }

  SECTION("Large offsets") {
    std::vector<off_t> offs = {4*GB, 5*GB, 10*GB};

    BloomSkipListWriter writer(offs);
    std::string data = writer.Serialize();

    REQUIRE(data[0] == (char) BLOOM_SKIP_LIST_FIRST_BYTE);

    std::cout << "large offsets" << std::endl;
    BloomSkipList skiplist;
    skiplist.Load((const uint8_t *)data.data());
    REQUIRE(skiplist.Size() == offs.size());

    for (std::size_t i = 0; i < offs.size(); i++) {
      REQUIRE(skiplist[i] == offs[i]);
    }
  }
}


TEST_CASE( "Varint large number", "[varint]" ) {
  VarintBuffer buf;
  off_t off = 5 * GB;
  buf.Append(off);

  std::string data = buf.Data();
  VarintIterator it(buf, 1);

  off_t off2 = it.Pop();
  REQUIRE(off == off2);

}




