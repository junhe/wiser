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

  SECTION("SkipIndex") {
    SkipIndex index;
    index.vec.emplace_back(3, 8);
    index.vec.emplace_back(10, 20);

    std::string buf = index.Serialize();

    SkipIndex index2;
    index2.Deserialize(buf, 0);

    REQUIRE(index.vec.size() == index2.vec.size());
    for (int i = 0; i < index.vec.size(); i++) {
      REQUIRE(index.vec[i].prev_doc_id == index2.vec[i].prev_doc_id);
      REQUIRE(index.vec[i].start_offset == index2.vec[i].start_offset);
    }
  }
}

