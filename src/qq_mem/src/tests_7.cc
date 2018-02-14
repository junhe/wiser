#include "catch.hpp"

#include <memory>

#include "utils.h"
#include "compression.h"

#include "test_helpers.h"


TEST_CASE( "Serialization", "[serial]" ) {
  std::string fake_padding = "012";
  SECTION("VarintBuffer") {
    VarintBuffer buf;
    buf.Append(11);
    buf.Append(12);

    std::string serialized = buf.Serialize();
    REQUIRE(serialized.size() == 3);

    VarintBuffer buf2;
    buf2.Deserialize(fake_padding + serialized, fake_padding.size());
    REQUIRE(buf.Size() == buf2.Size());
    REQUIRE(buf.Data() == buf2.Data());
  }

  SECTION("SpanMeta") {
    SpanMeta meta(3, 8);
    std::string buf = meta.Serialize();

    SpanMeta meta2(fake_padding + buf, fake_padding.size());
    REQUIRE(meta2.prev_doc_id == meta.prev_doc_id);
    REQUIRE(meta2.start_offset == meta.start_offset);
  }

  SECTION("SkipIndex") {
    SkipIndex index;
    index.vec.emplace_back(3, 8);
    index.vec.emplace_back(10, 20);

    std::string buf = index.Serialize();

    SkipIndex index2;
    index2.Deserialize(fake_padding + buf, fake_padding.size());

    REQUIRE(index.vec.size() == index2.vec.size());
    for (int i = 0; i < index.vec.size(); i++) {
      REQUIRE(index.vec[i].prev_doc_id == index2.vec[i].prev_doc_id);
      REQUIRE(index.vec[i].start_offset == index2.vec[i].start_offset);
    }
  }

  SECTION("Posting list") {
    PostingListDelta pl = create_posting_list_delta(2); 
    std::string buf = pl.Serialize();

    PostingListDelta pl2("hello");
    pl2.Deserialize(fake_padding + buf, fake_padding.size());

    REQUIRE(pl.Size() == pl2.Size());
    REQUIRE(pl.ByteCount() == pl2.ByteCount());
    REQUIRE(pl.GetSkipIndex() == pl2.GetSkipIndex());
    REQUIRE(pl == pl2);
    REQUIRE(pl2.Serialize() == buf);
  }
}

