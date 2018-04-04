#include "catch.hpp"

#include "packed_value.h"

TEST_CASE( "PackedInts", "[qqflash]" ) {
  SECTION("NumBitsInByte") {
    REQUIRE(NumBitsInByte(0) == 8);
    REQUIRE(NumBitsInByte(1) == 7);
    REQUIRE(NumBitsInByte(7) == 1);
    REQUIRE(NumBitsInByte(8) == 8);
  }

  SECTION("Max bits per value") {
    PackedIntsWriter writer;

    REQUIRE(writer.MaxBitsPerValue() == 0);

    writer.Add(0x01);
    REQUIRE(writer.MaxBitsPerValue() == 1);

    writer.Add(0x01);
    REQUIRE(writer.MaxBitsPerValue() == 1);

    writer.Add(0x0F);
    REQUIRE(writer.MaxBitsPerValue() == 4);

    writer.Add(0x01);
    REQUIRE(writer.MaxBitsPerValue() == 4);
  }

  SECTION("Append to byte") {
    uint8_t buf[8];
    memset(buf, 0, 8);

    SECTION("Only one bit") {
      int next = AppendToByte(1, 1, buf, 0);
      REQUIRE(buf[0] == (const uint8_t)(0x01 << 7));
      REQUIRE(next == 1);
    }

    SECTION("Two bits") {
      int next = AppendToByte(1, 1, buf, 0);
      REQUIRE(buf[0] == (const uint8_t)(0x01 << 7));
      REQUIRE(next == 1);

      next = AppendToByte(1, 1, buf, 1);
      REQUIRE(buf[0] == (const uint8_t)(0x03 << 6));
      REQUIRE(next == 2);
    }

    SECTION("Exact one byte") {
      int next = AppendToByte(0xFF, 8, buf, 0);
      REQUIRE(buf[0] == (const uint8_t)(0xFF));
      REQUIRE(next == 8);
    }

    SECTION("Append to the second byte") {
      int next = AppendToByte(0x01, 1, buf, 8);
      REQUIRE(buf[1] == (const uint8_t)(0x80));
      REQUIRE(next == 9);
    }
  }


  SECTION("Append value") {
    uint8_t buf[8];
    memset(buf, 0, 8);

    SECTION("Append a simple one") {
      int next = AppendValue(0x01, 1, buf, 0);
      REQUIRE(buf[0] == (const uint8_t)(0x80));
      REQUIRE(next == 1);
    }

    SECTION("Append two bits") {
      int next = AppendValue(0x01, 1, buf, 0);
      REQUIRE(buf[0] == (const uint8_t)(0x80));
      REQUIRE(next == 1);

      next = AppendValue(0x01, 1, buf, 1);
      REQUIRE(buf[0] == (const uint8_t)(0xC0));
      REQUIRE(next == 2);
    }

    SECTION("Exact one byte") {
      int next = AppendValue(0xFF, 8, buf, 0);
      REQUIRE(buf[0] == (const uint8_t)(0xFF));
      REQUIRE(next == 8);
    }

    SECTION("One byte across two bytes") {
      int next = AppendValue(0xFF, 8, buf, 4);
      REQUIRE(buf[0] == (const uint8_t)(0x0F));
      REQUIRE(buf[1] == (const uint8_t)(0xF0));
      REQUIRE(next == 12);
    }

    SECTION("Two bytes to two bytes") {
      int next = AppendValue(0xFFFF, 16, buf, 0);
      REQUIRE(buf[0] == (const uint8_t)(0xFF));
      REQUIRE(buf[1] == (const uint8_t)(0xFF));
      REQUIRE(next == 16);
    }
 
    SECTION("Two bytes to three bytes") {
      int next = AppendValue(0xFFFF, 16, buf, 4);
      REQUIRE(buf[0] == (const uint8_t)(0x0F));
      REQUIRE(buf[1] == (const uint8_t)(0xFF));
      REQUIRE(buf[2] == (const uint8_t)(0xF0));
      REQUIRE(next == 20);
    }
  }

  SECTION("Serialize") {
    PackedIntsWriter writer;

    SECTION("All values are 0") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(0);
      }
      REQUIRE(writer.MaxBitsPerValue() == 1);

      std::string data = writer.Serialize();
      REQUIRE(data.size() == (128 / 8)); // 128 bits in total

      for (int i = 0; i < data.size(); i++) {
        REQUIRE(data[i] == 0);
      }
    }

    SECTION("All values are 1") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(1);
      }
      REQUIRE(writer.MaxBitsPerValue() == 1);

      std::string data = writer.Serialize();
      REQUIRE(data.size() == (128 / 8)); // 128 bits in total

      for (int i = 0; i < data.size(); i++) {
        REQUIRE(data[i] == (char)0xff);
      }
    }
  }
}


