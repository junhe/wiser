#include "catch.hpp"

#include "query_pool.h"
#include "packed_value.h"
#include "flash_engine_dumper.h"

extern "C" {
#include "bitpacking.h"
}

TEST_CASE( "Query pools", "[qpool]" ) {

  SECTION("QueryPool") {
    QueryPool pool;
    pool.Add(SearchQuery({"hello"}));
    pool.Add(SearchQuery({"world"}));
    REQUIRE(pool.Size() == 2);

    REQUIRE(pool.Next().terms == TermList{"hello"});
    REQUIRE(pool.Next().terms == TermList{"world"});
    REQUIRE(pool.Next().terms == TermList{"hello"});
    REQUIRE(pool.Next().terms == TermList{"world"});
  }

  SECTION("Query pool array") {
    QueryPoolArray pool(3);
    pool.Add(0, SearchQuery({"hello"}));
    pool.Add(1, SearchQuery({"world"}));
    pool.Add(2, SearchQuery({"!"}));
    REQUIRE(pool.Size() == 3);

    REQUIRE(pool.Next(0).terms == TermList{"hello"});
    REQUIRE(pool.Next(0).terms == TermList{"hello"});
    REQUIRE(pool.Next(1).terms == TermList{"world"});
    REQUIRE(pool.Next(1).terms == TermList{"world"});
    REQUIRE(pool.Next(2).terms == TermList{"!"});
    REQUIRE(pool.Next(2).terms == TermList{"!"});
  }

  SECTION("Query producer from realistic log") {
    auto path = "src/testdata/query_log_with_phrases";
    QueryProducerByLog producer(path, 2);

    auto q = producer.NextNativeQuery(0);
    REQUIRE(q.terms == TermList{"greek", "armi"});
    REQUIRE(q.is_phrase == true);

    q = producer.NextNativeQuery(1);
    REQUIRE(q.terms == TermList{"nightt", "rain", "nashvil"});
    REQUIRE(q.is_phrase == false);
  }

  SECTION("Query producer from realistic log with lock") {
    auto path = "src/testdata/query_log_with_phrases";
    QueryProducerNoLoop producer(path);

    auto q = producer.NextNativeQuery(0);
    REQUIRE(q.terms == TermList{"greek", "armi"});
    REQUIRE(q.is_phrase == true);

    q = producer.NextNativeQuery(0);
    REQUIRE(q.terms == TermList{"nightt", "rain", "nashvil"});
    REQUIRE(q.is_phrase == false);

    REQUIRE(producer.Size() == 10);

    for (int i = 0; i < 8; i++) {
      REQUIRE(producer.IsEnd() == false);
      q = producer.NextNativeQuery(0);
    }
    REQUIRE(producer.IsEnd() == true);

  }

}


TEST_CASE( "Little packed ints", "[pack]" ) {
  SECTION("lib") {
    uint32_t original_array[128];
    uint8_t buf[128 * 4];
    uint32_t unpacked[128];

    for (int i = 0; i < 128; i++) {
      original_array[i] = i;
    }

    turbopack32(original_array, 128, 8, buf);
    turbounpack32(buf, 128, 8, unpacked);

    for (int i = 0; i < 128; i++) {
      REQUIRE(unpacked[i] == i);
    }
  }

  SECTION("Write and Read all 0") {
    LittlePackedIntsWriter writer;

    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      writer.Add(0);  
    }

    std::string data = writer.Serialize();

    LittlePackedIntsReader reader((uint8_t *)data.data());
    REQUIRE(reader.NumBits() == 1);
    reader.DecodeToCache();
    
    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      REQUIRE(reader.Get(i) == 0);
    }
  }

  SECTION("Write and Read all 1") {
    LittlePackedIntsWriter writer;

    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      writer.Add(1); 
    }

    std::string data = writer.Serialize();

    LittlePackedIntsReader reader((uint8_t *)data.data());
    REQUIRE(reader.NumBits() == 1);
    reader.DecodeToCache();
    
    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      REQUIRE(reader.Get(i) == 1);
    }
  }

  SECTION("Write and Read") {
    LittlePackedIntsWriter writer;

    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      writer.Add(i);  
    }

    std::string data = writer.Serialize();
    REQUIRE(data.size() == 112 + 2); // 2 is the size of the header

    LittlePackedIntsReader reader((uint8_t *)data.data());
    REQUIRE(reader.NumBits() == 7);
    reader.DecodeToCache();
    
    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      REQUIRE(reader.Get(i) == i);
    }
  }

  SECTION("Write and Read rand numbers") {
    LittlePackedIntsWriter writer;

    srand(0);
    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      writer.Add(rand() % 10000000); 
    }

    std::string data = writer.Serialize();

    LittlePackedIntsReader reader((uint8_t *)data.data());
    reader.DecodeToCache();
    
    srand(0);
    for (int i = 0; i < PACK_ITEM_CNT; i++) {
      REQUIRE(reader.Get(i) == rand() % 10000000);
    }
  }
}


void CheckEncoding(uint32_t n_pages, off_t pl_offset) {
  uint32_t n_pages_decoded;
  off_t pl_offset_decoded;
  off_t off_in_index;
    
  off_in_index = EncodePrefetchZoneAndOffset(n_pages, pl_offset);
  DecodePrefetchZoneAndOffset(off_in_index, &n_pages_decoded, &pl_offset_decoded);

  std::cout << "off_in_index: " << off_in_index << std::endl;
  std::cout << "n_pages: " << n_pages << std::endl;
  std::cout << "n_pages_decoded: " << n_pages_decoded << std::endl;
  std::cout << "pl_offset:" << pl_offset << std::endl;
  std::cout << "pl_offset_decoded:" << pl_offset_decoded << std::endl;

  REQUIRE(n_pages == n_pages_decoded);
  REQUIRE(pl_offset == pl_offset_decoded);
}


TEST_CASE( "Encoding prefetch zone and posting list start", "[dump00]" ) {
  CheckEncoding(0, 0);
  CheckEncoding(1, 1);
  CheckEncoding(128, 8223776);
  CheckEncoding(1 << 15, (uint64_t) 1 << 47); // use the max bit
  CheckEncoding(5*MB/(4*KB), 50L*GB);   
  CheckEncoding(100*MB/(4*KB), 100L*GB); 
  CheckEncoding(100*MB/(4*KB), 0);   
  CheckEncoding(0, 100L*GB - 888888);   
}


void CheckCompression(std::string text) {
  const std::size_t buf_size = 16 * KB;
  // const std::size_t buf_size = 16;
  char *buf = (char *) malloc(buf_size);

  std::string compressed = CompressText(text, buf, buf_size); 
  std::string decompressed = DecompressText(compressed.data(), buf, buf_size);
  REQUIRE(text == decompressed);
}


std::string GetRandomText(int n) {
  srand(0);
  std::string ret(n, '0');
  for (int i = 0; i < n; i++) {
    char ch = rand() % 128;
    ret[i] = ch; 
  }
  return ret;
}


TEST_CASE( "Compress and decompress using small buffer", "[doc_store]" ) {
  SECTION("Simple") {
    CheckCompression("Hello!");
    CheckCompression("H");
    CheckCompression("hello world is a program that does some output.");
  }
  
  SECTION("Large ones") {
    CheckCompression(GetRandomText(100));
    CheckCompression(GetRandomText(1000));
    CheckCompression(GetRandomText(10000));
    CheckCompression(GetRandomText(100000));
    CheckCompression(GetRandomText(100*KB));
    CheckCompression(GetRandomText(600*KB));
    CheckCompression(GetRandomText(1*MB));
  }
}


TEST_CASE( "Chunked store", "[doc_store0]" ) {
  SECTION("Should align") {
    REQUIRE(ShouldAlign(0, 1) == false);
    REQUIRE(ShouldAlign(0, 4096) == false);
    REQUIRE(ShouldAlign(1, 4096) == true);
    REQUIRE(ShouldAlign(1, 4096 * 2) == true);
  }

  SECTION("Chunked doc store, one doc") {
    ChunkedDocStoreDumper store;
    store.Add(0, "hello");
    store.Dump("/tmp/tmp.fdx", "/tmp/tmp.fdt");

    ChunkedDocStoreReader reader;
    reader.Load("/tmp/tmp.fdx", "/tmp/tmp.fdt");
    std::string text = reader.Get(0);
    REQUIRE(text == "hello");
  }

  SECTION("Chunked doc store, many docs") {
    ChunkedDocStoreDumper store;
    int n_docs = 100;
    std::map<int, std::string> origins;

    for (int i = 0; i < n_docs; i++) {
      std::string text = GetRandomText(i + 50 * KB);
      store.Add(i, text);
      origins[i] = text;
    }

    store.Dump("/tmp/tmp.fdx", "/tmp/tmp.fdt");

    ChunkedDocStoreReader reader;
    reader.Load("/tmp/tmp.fdx", "/tmp/tmp.fdt");

    for (int i = 0; i < n_docs; i++) {
      std::string text = reader.Get(i);
      REQUIRE(text == origins[i]);
    }
  }
}



