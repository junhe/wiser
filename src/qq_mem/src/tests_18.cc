#include "catch.hpp"

#include <iostream>

#include "bloom_filter.h"
#include "flash_containers.h"

TEST_CASE( "Bloom box skip list", "[bloomfilter]" ) {
  BloomSkipListWriter writer({10});
  std::string data = writer.Serialize();

  REQUIRE(data[0] == (char) BLOOM_SKIP_LIST_FIRST_BYTE);


}

