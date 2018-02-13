#include "catch.hpp"

#include <memory>

#include "utils.h"
#include "compression.h"

#include "test_helpers.h"


TEST_CASE( "Serialization", "[serial]" ) {
  SECTION("VarintBuffer") {
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

  SECTION("SpanMeta") {
    SpanMeta meta(3, 8);
    std::string buf = meta.Serialize();

    SpanMeta meta2(buf, 0);
    REQUIRE(meta2.prev_doc_id == meta.prev_doc_id);
    REQUIRE(meta2.start_offset == meta.start_offset);
  }

}

