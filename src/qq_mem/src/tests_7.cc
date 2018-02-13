#include "catch.hpp"

#include <memory>

#include "utils.h"
#include "compression.h"

#include "test_helpers.h"


TEST_CASE( "Serialization", "[serial]" ) {
  VarintBuffer buf;
  buf.Append(11);
  buf.Append(12);

  std::string serialized = buf.Serialize();
  REQUIRE(serialized.size() == 3);

  VarintBuffer buf2;
  buf2.Deserialize(serialized, 0);
  REQUIRE(buf.Size() == buf2.Size());
  REQUIRE(buf.Data() == buf2.Data());
}


