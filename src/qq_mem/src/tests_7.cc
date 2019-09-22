#include "catch.hpp"

#include <memory>

#include "utils.h"
#include "compression.h"
#include "qq_mem_engine.h"
#include "doc_store.h"
#include "doc_length_store.h"

#include "test_helpers.h"


TEST_CASE( "Serialization", "[serial]" ) {
  std::string fake_padding = "012";

  SECTION("Empty VarintBuffer") {
    VarintBuffer buf;

    std::string serialized = buf.Serialize();
    REQUIRE(serialized.size() == 1);

    VarintBuffer buf2;
    buf2.Deserialize(fake_padding + serialized, fake_padding.size());
    REQUIRE(buf.Size() == buf2.Size());
    REQUIRE(buf.Data() == buf2.Data());
  }


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
    for (std::size_t i = 0; i < index.vec.size(); i++) {
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

  SECTION("Empty inverted index") {
    InvertedIndexQqMemDelta index;
    index.Serialize("/tmp/inverted_index.dump");

    InvertedIndexQqMemDelta index2;
    index2.Deserialize("/tmp/inverted_index.dump");
    
    REQUIRE(index == index2);
  }


  SECTION("Inverted index") {
    InvertedIndexQqMemDelta index;
    {
      DocInfo info("hello world hello", "hello world", "", "", "TOKEN_ONLY");
      index.AddDocument(0, info);
    }

    {
      DocInfo info("bb cd", "xj sj", "", "", "TOKEN_ONLY");
      index.AddDocument(1, info);
    }

    {
      DocInfo info("jj fjsjj 2jsd", "sj 2j3j xj", "", "", "TOKEN_ONLY");
      index.AddDocument(2, info);
    }

    index.Serialize("/tmp/inverted_index.dump");

    InvertedIndexQqMemDelta index2;
    index2.Deserialize("/tmp/inverted_index.dump");
    
    REQUIRE(index == index2);
  }

  SECTION("Doc Length Store") {
    DocLengthStore store;       
    store.AddLength(8, 88);
    store.AddLength(100, 1000);
    store.Serialize("/tmp/doc_length.dump");

    DocLengthStore store2;
    store2.Deserialize("/tmp/doc_length.dump");
    REQUIRE(store2.GetLength(8) == store.GetLength(8));
    REQUIRE(store2.GetLength(100) == store.GetLength(100));
    REQUIRE(store2.GetAvgLength() == store.GetAvgLength());
  }

  SECTION("Doc Length Char Store") {
    DocLengthCharStore store;       
    store.AddLength(8, 88);
    store.AddLength(100, 1000);
    store.Serialize("/tmp/doc_char_length.dump2");

    DocLengthCharStore store2;
    store2.Deserialize("/tmp/doc_char_length.dump2");
    REQUIRE(store2.GetLength(8) == store.GetLength(8));
    REQUIRE(store2.GetLength(100) == store.GetLength(100));
    REQUIRE(store2.GetAvgLength() == store.GetAvgLength());
  }

}

TEST_CASE( "Whole engine serialization test", "[serial]" ) {
  SECTION("The whole engine") {
    QqMemEngineDelta engine;

    DocInfo info("hello world hello", "hello world", "", "", "TOKEN_ONLY");
    engine.AddDocument(info);

    engine.Serialize("/tmp/test_folder");

    QqMemEngineDelta engine2;
    engine2.Deserialize("/tmp/test_folder");

    REQUIRE(engine == engine2);
  }

  SECTION("The whole engine, a lot docs") {
    QqMemEngineDelta engine;

    for (int i = 0; i < 1000; i++) {
      DocInfo info("hello world hello", 
          "hello world " + std::to_string(i), "", "", "TOKEN_ONLY");
      engine.AddDocument(info);
    }

    engine.Serialize("/tmp/test_folder");

    QqMemEngineDelta engine2;
    engine2.Deserialize("/tmp/test_folder");

    REQUIRE(engine == engine2);
  }

  SECTION("The whole engine, real docs") {
    std::string path = "/tmp/test_folder";
    utils::RemoveDir(path);

    QqMemEngineDelta engine;

    engine.LoadLocalDocuments("src/testdata/line_doc_with_positions", 1000, 
        "WITH_POSITIONS");

    engine.Serialize(path);

    QqMemEngineDelta engine2;
    engine2.Deserialize(path);

    REQUIRE(engine == engine2);
  }
}

