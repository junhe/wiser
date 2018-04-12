#include "catch.hpp"

#include "flash_iterators.h"


TEST_CASE( "VInts writing and reading", "[qqflash]" ) {
  VIntsWriter writer;

  SECTION("Empty") {
    std::string buf = writer.Serialize();

    VIntsReader reader((const uint8_t *)buf.data());
    REQUIRE(reader.IsEnd() == true);
  }

  SECTION("One number") {
    writer.Append(5);
    std::string buf = writer.Serialize();

    VIntsReader reader((const uint8_t *) buf.data());
    REQUIRE(reader.IsEnd() == false);

    REQUIRE(reader.Pop() == 5);
    REQUIRE(reader.IsEnd() == true);
  }

  SECTION("Three numbers") {
    writer.Append(1000);
    writer.Append(2);
    writer.Append(13377200);
    std::string buf = writer.Serialize();

    VIntsReader reader((const uint8_t *) buf.data());
    REQUIRE(reader.IsEnd() == false);

    REQUIRE(reader.Pop() == 1000);
    REQUIRE(reader.Pop() == 2);
    REQUIRE(reader.Pop() == 13377200);
    REQUIRE(reader.IsEnd() == true);
  }
}


