#include "catch.hpp"

#include "flash_iterators.h"
#include "utils.h"

std::string EncodeToDeltaEncodedPackedInts(const std::vector<uint32_t> &values) {
  std::vector<uint32_t> deltas = EncodeDelta(values);

  LittlePackedIntsWriter writer;
  for (auto &delta : deltas) {
    writer.Add(delta);
  }
  return writer.Serialize();
}


std::string EncodeToDeltaEncodedVInts(const std::vector<uint32_t> &values) {
  std::vector<uint32_t> deltas = EncodeDelta(values);

  VIntsWriter writer;
  for (auto &delta : deltas) {
    writer.Append(delta);
  }

  std::string buf = writer.Serialize();

  return writer.Serialize();
}


int PsudoIncreasingRandom(int i) {
  int seed = 6263;
  int rand = (i * seed + 12345) % 23;
  return 1 + i * 30 + rand;
}


TEST_CASE( "Delta Encoded PackedIntsIterator", "[qqflash]" ) {
  SECTION("Simple sequence, read by Reader") {
    std::vector<uint32_t> values;
    for (int i = 0; i < PACK_SIZE; i++) {
      values.push_back(i);
    }

    std::string buf = EncodeToDeltaEncodedPackedInts(values);

    DeltaEncodedPackedIntsIterator it((const uint8_t *)buf.data(), 0);

    SECTION("Advance()") {
      for (int i = 0; i < PACK_SIZE; i++) {
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == values[i]);
        it.Advance();
      }
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo()") {
      for (int i = 0; i < PACK_SIZE; i++) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == values[i]);
      }
      it.Advance();
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo() with stides") {
      for (int i = 0; i < PACK_SIZE; i += 3) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == values[i]);
      }
      it.SkipTo(PACK_SIZE);
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SkipForward() to the start") {
      it.SkipForward(0);
      REQUIRE(it.Value() == 0);
      REQUIRE(it.Index() == 0);
      REQUIRE(it.IsEnd() == false);

      it.SkipForward(100);
      REQUIRE(it.Value() == 100);
      REQUIRE(it.Index() == 100);
      REQUIRE(it.IsEnd() == false);

      it.SkipForward(1000);
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SkipForward() to the end") {
      it.SkipForward(PACK_SIZE - 1);
      REQUIRE(it.Value() == PACK_SIZE - 1);
      REQUIRE(it.Index() == PACK_SIZE - 1);
      REQUIRE(it.IsEnd() == false);
    }

    SECTION("SkipForward() backwards") {
      it.SkipForward(50);
      REQUIRE(it.Value() == 50);
      REQUIRE(it.Index() == 50);

      it.SkipForward(0);
      REQUIRE(it.Value() == 50);
      REQUIRE(it.Index() == 50);
    }
  }

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
        REQUIRE(it.Value() == values[i]);
        it.Advance();
      }
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo()") {
      for (int i = 0; i < PACK_SIZE; i++) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == values[i]);
      }
      it.Advance();
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo() with stides") {
      for (int i = 0; i < PACK_SIZE; i += 3) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Value() == values[i]);
      }
      it.SkipTo(PACK_SIZE);
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SkipForward() simple") {
      it.SkipForward(values[10]);
      REQUIRE(it.Value() == values[10]);
      REQUIRE(it.Index() == 10);
      REQUIRE(it.IsEnd() == false);

      it.SkipForward(values[15] - 1);
      REQUIRE(it.Value() == values[15]);
      REQUIRE(it.Index() == 15);
      REQUIRE(it.IsEnd() == false);
    }
  }
}


TEST_CASE( "Delta Encoded VInts Iterator", "[qqflash][deltavints]" ) {
  SECTION("Simple numbers") {
    std::vector<uint32_t> values;
    const int CNT = 88;
    for (int i = 0; i < CNT; i++) {
      values.push_back(i);
    }

    std::string buf = EncodeToDeltaEncodedVInts(values);

    DeltaEncodedVIntsIterator it((const uint8_t *)buf.data(), 0);

    SECTION("Pop()") {
      for (int i = 0; i < CNT; i++) {
        REQUIRE(it.Index() == i);
        REQUIRE(it.Pop() == i);
      }
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo()") {
      for (int i = 0; i < CNT; i++) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Peek() == i);
      }
      it.Pop();
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo() with strides") {
      for (int i = 0; i < CNT; i += 3) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Peek() == i);
      }
      it.SkipTo(CNT);
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SkipForward() Simple") {
      it.SkipForward(19);
      REQUIRE(it.Index() == 19);
      REQUIRE(it.Peek() == 19);

      it.SkipForward(1);
      REQUIRE(it.Index() == 19);
      REQUIRE(it.Peek() == 19);

      it.SkipForward(100);
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SkipForward() to the first item") {
      it.SkipForward(0);
      REQUIRE(it.Index() == 0);
      REQUIRE(it.Peek() == 0);
      REQUIRE(it.IsEnd() == false);
    }

    SECTION("SkipForward() to the last item") {
      it.SkipForward(CNT - 1);
      REQUIRE(it.Index() == CNT - 1);
      REQUIRE(it.Peek() == CNT - 1);
      REQUIRE(it.IsEnd() == false);

      it.SkipForward(100);
      REQUIRE(it.IsEnd() == true);
    }
    
  }

  SECTION("Increasing psuedo random numbers") {
    std::vector<uint32_t> values;
    const int CNT = 88;
    for (int i = 0; i < CNT; i++) {
      values.push_back(PsudoIncreasingRandom(i));
    }

    std::string buf = EncodeToDeltaEncodedVInts(values);

    DeltaEncodedVIntsIterator it((const uint8_t *)buf.data(), 0);

    SECTION("Pop()") {
      for (int i = 0; i < CNT; i++) {
        REQUIRE(it.Index() == i);
        REQUIRE(it.Pop() == values[i]);
      }
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo()") {
      for (int i = 0; i < CNT; i++) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Peek() == values[i]);
      }
      it.Pop();
      REQUIRE(it.IsEnd() == true);
    }

    SECTION("SKipTo() with strides") {
      for (int i = 0; i < CNT; i += 3) {
        it.SkipTo(i);
        REQUIRE(it.Index() == i);
        REQUIRE(it.Peek() == PsudoIncreasingRandom(i));
      }
      it.SkipTo(CNT);
      REQUIRE(it.IsEnd() == true);
    }
  }
}





