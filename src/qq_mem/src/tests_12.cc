#include "catch.hpp"

#include "flash_iterators.h"
#include "utils.h"

std::string EncodeToDeltaEncodedPackedInts(const std::vector<uint32_t> &values) {
  std::vector<uint32_t> deltas = EncodeDelta(values);

  PackedIntsWriter writer;
  for (auto &delta : deltas) {
    writer.Add(delta);
  }
  return writer.Serialize();
}


int PsudoIncreasingRandom(int i) {
  int seed = 6263;
  int rand = (i * seed + 12345) % 23;
  return 1 + i * 30 + rand;
}

TEST_CASE( "Delta Encoded PackedIntsIterator", "[qqflash]" ) {
  SECTION("Psuedo random numbers, read by Reader") {
    std::vector<uint32_t> values;
    for (int i = 0; i < PACK_SIZE; i++) {
      values.push_back(PsudoIncreasingRandom(i));
    }

    std::string buf = EncodeToDeltaEncodedPackedInts(values);

    DeltaEncodedPackedIntsIterator it((const uint8_t *)buf.data(), 0);

    SECTION("Advance()") {
      for (int i = 0; i < PACK_SIZE; i++) {
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == PsudoIncreasingRandom(i));
        it.Advance();
      }
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo()") {
      for (int i = 0; i < PACK_SIZE; i++) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == PsudoIncreasingRandom(i));
      }
      it.Advance();
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo() with stides") {
      for (int i = 0; i < PACK_SIZE; i += 3) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == PsudoIncreasingRandom(i));
      }
      it.SkipTo(PACK_SIZE);
      REQUIRE(it.IsEnd() == true);
    }
  }
}

