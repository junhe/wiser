#include "catch.hpp"

#include "packed_value.h"

TEST_CASE( "PackedInts utilities", "[qqflash]" ) {
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

  SECTION("Extracting bits") {
    uint8_t buf[8];
    memset(buf, 0, 8);

    SECTION("Just 1") {
      buf[0] = 0x80;
      REQUIRE(ExtractBits(buf, 0, 1) == 1);
    }

    SECTION("Just 0") {
      buf[0] = 0x00;
      REQUIRE(ExtractBits(buf, 0, 1) == 0);
    }

    SECTION("Across two bytes") {
      buf[0] = 0x04;
      buf[1] = 0xF0;
      REQUIRE(ExtractBits(buf, 4, 8) == 0x4F);
    }

    SECTION("Across 8 bytes") {
      memset(buf, 0xFF, 8);
      REQUIRE(ExtractBits(buf, 0, 64) == ~((long)0x00));
    }

    SECTION("Extract second bit") {
      // We used to have a bug for this case
      memset(buf, 0xFF, 8);
      REQUIRE(ExtractBits(buf, 1, 1) == 1);
    }
  }
}


TEST_CASE( "PackedInts", "[qqflash]" ) {

  int pack_size = PackedIntsWriter::PACK_SIZE;
  SECTION("Serialize") {
    PackedIntsWriter writer;

    SECTION("All values are 0") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(0);
      }
      REQUIRE(writer.MaxBitsPerValue() == 1);

      std::string data = writer.Serialize();
      // 128 bits in total, + 1 byte for the header
      REQUIRE(data.size() == (128 / 8 + 1)); 

      REQUIRE(data[0] == 1); // the header
      for (int i = 1; i < data.size(); i++) {
        REQUIRE(data[i] == 0);
      }
    }

    SECTION("All values are 1") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(1);
      }
      REQUIRE(writer.MaxBitsPerValue() == 1);

      std::string data = writer.Serialize();
      REQUIRE(data.size() == (128 / 8 + 1)); // 128 bits in total

      REQUIRE(data[0] == 1); // the header
      for (int i = 1; i < data.size(); i++) {
        REQUIRE(data[i] == (char)0xff);
      }
    }

    SECTION("All values are 0") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(0);
      }
      REQUIRE(writer.MaxBitsPerValue() == 1);

      std::string data = writer.Serialize();

      SECTION("Read by Reader") {
        PackedIntsReader reader((const uint8_t *)data.data());
        for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
          REQUIRE(reader.Get(i) == 0);
        }
      }

      SECTION("Read by iterator") {
        PackedIntsIterator it((const uint8_t *)data.data());
        
        int cnt = 0;
        while (it.IsEnd() == false) {
          REQUIRE(it.Value() == 0);
          it.Advance();
          cnt++;
        }

        REQUIRE(cnt == pack_size);
      }
    }

    SECTION("All values are 1") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(1);
      }
      REQUIRE(writer.MaxBitsPerValue() == 1);

      std::string data = writer.Serialize();

      SECTION("Read by reader") {
        PackedIntsReader reader((const uint8_t *)data.data());

        for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
          REQUIRE(reader.Get(i) == 1);
        }
      }

      SECTION("Read by iterator") {
        PackedIntsIterator it((const uint8_t *)data.data());
        
        int cnt = 0;
        while (it.IsEnd() == false) {
          REQUIRE(it.Value() == 1);
          it.Advance();
          cnt++;
        }

        REQUIRE(cnt == pack_size);
      }
    }

    SECTION("Psuedo random numbers, read by Reader") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(i * 10 % 7);
      }
      REQUIRE(writer.MaxBitsPerValue() == utils::NumOfBits(6)); // max number is 6

      std::string data = writer.Serialize();

      SECTION("read by reader") {
        PackedIntsReader reader((const uint8_t *)data.data());
        for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
          REQUIRE(reader.Get(i) == (i * 10 % 7));
        }
      }

      SECTION("read by iterator") {
        PackedIntsIterator it((const uint8_t *)data.data());
        for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
          it.SkipTo(i);
          REQUIRE(it.Value() == (i * 10 % 7));
        }
      }

      SECTION("read by iterator, using Advance()") {
        PackedIntsIterator it((const uint8_t *)data.data());
        for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
          REQUIRE(it.Value() == (i * 10 % 7));
          it.Advance();
        }

        REQUIRE(it.IsEnd() == true);
      }

      SECTION("read by iterator, large strides") {
        PackedIntsIterator it((const uint8_t *)data.data());
        for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i += 10) {
          it.SkipTo(i);
          REQUIRE(it.Value() == (i * 10 % 7));
        }
      }
    }

    SECTION("Psuedo large random numbers, read by Reader") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(i * 1000 % 9973); // 9973 is just a prime number
      }
      std::string data = writer.Serialize();

      PackedIntsReader reader((const uint8_t *)data.data());
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        REQUIRE(reader.Get(i) == (i * 1000 % 9973));
      }
    }
 
    SECTION("Full bits, read by Reader") {
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        writer.Add(~(long)0x00); 
      }
      std::string data = writer.Serialize();

      PackedIntsReader reader((const uint8_t *)data.data());
      for (int i = 0; i < PackedIntsWriter::PACK_SIZE; i++) {
        REQUIRE(reader.Get(i) == ~(long)0x00);
      }
    }
  }
}


