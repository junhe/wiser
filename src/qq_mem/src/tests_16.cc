#include "catch.hpp"

#include "query_pool.h"
#include "packed_value.h"
#include "flash_engine_dumper.h"
#include "bloom.h"
#include "bloom_filter.h"
#include "engine_factory.h"


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


TEST_CASE( "Bloom Filter", "[bloomfilter]" ) {
  Bloom bloom;
  int expected_entries = 2;
  float ratio = 0.01;
  SECTION("Store the bloom filter") {
    // init bloom filter (expected added keys, ratio of 
    // false-positive(lower, more reliable))
    bloom_init(&bloom, expected_entries, ratio);
    
    // add keys (string, length of string)
    bloom_add(&bloom, "hello", 6);
    bloom_add(&bloom, "wisconsin", 10);

    // check keys
    REQUIRE(bloom_check(&bloom, "hello", 6) == 1);
    REQUIRE(bloom_check(&bloom, "wisconsin", 10) == 1);
    REQUIRE(bloom_check(&bloom, "random", 7) == 0);
    REQUIRE(bloom_check(&bloom, "helle", 6) == 0);
    REQUIRE(bloom_check(&bloom, "wisconson", 6) == 0);
    bloom_print(&bloom);

    // set with know bit array
    Bloom bloom_test;
    bloom_set(&bloom_test, expected_entries, ratio, bloom.bf);
    
    // check keys
    REQUIRE(bloom_check(&bloom_test, "hello", 6) == 1);
    REQUIRE(bloom_check(&bloom_test, "wisconsin", 10) == 1);
    REQUIRE(bloom_check(&bloom_test, "random", 7) == 0);
    REQUIRE(bloom_check(&bloom_test, "helle", 6) == 0);
    bloom_print(&bloom_test);

    bloom_free(&bloom);
  }
}


typedef std::pair<std::set<std::string>, std::set<std::string>> set_pair ;


set_pair GetRandSets(std::size_t n) {
  std::set<std::string> in, out;
  
  while (in.size() < n) {
    in.insert(std::to_string(rand()));
  }

  while (out.size() < n) {
    std::string s = std::to_string(rand());
    if (out.count(s) == 0)
      out.insert(s);
  }

  return set_pair(in, out);
}

void CheckInitializedFilter(Bloom *blm, const set_pair &setpair) {
  for (auto &s : setpair.first) {
    REQUIRE(CheckBloom(blm, s) == BLM_MAY_PRESENT);
  }

  for (auto &s : setpair.second) {
    REQUIRE(CheckBloom(blm, s) == BLM_NOT_PRESENT);
  }
}

void CheckBloomFilter(std::size_t n) {
  float ratio = 0.00001;
  set_pair setpair = GetRandSets(n);
  
  std::vector<std::string> vec;
  for (auto &s : setpair.first) {
    vec.push_back(s);
  }

  Bloom bloom = CreateBloom(ratio, vec);

  CheckInitializedFilter(&bloom, setpair);
  
	// Load and check
  Bloom bloom_test;
  bloom_set(&bloom_test, n, ratio, bloom.bf);

  CheckInitializedFilter(&bloom_test, setpair);
  FreeBloom(&bloom);
}


TEST_CASE( "Bloom Filter, extended tests", "[bloomfilter]" ) {
  srand(1);
	CheckBloomFilter(1);
	CheckBloomFilter(2);
	CheckBloomFilter(3);

	CheckBloomFilter(10);
	CheckBloomFilter(100);
}


TEST_CASE( "Check string", "[utils]" ) {
  REQUIRE(utils::StartsWith("a", "a") == true);
  REQUIRE(utils::StartsWith("ab", "a") == true);
  REQUIRE(utils::StartsWith("ab", "ab") == true);
  REQUIRE(utils::StartsWith("abc", "ab") == true);

  REQUIRE(utils::StartsWith("a", "b") == false);
  REQUIRE(utils::StartsWith("abc", "abd") == false);
}


TEST_CASE( "Line doc parser for phrase ends", "[engine]" ) {
  LineDocParserPhraseEnd parser("src/testdata/line_doc.with-bloom.toy");
  DocInfo info;
  std::vector<DocInfo> info_arr;

  int cnt = 0;
  while (parser.Pop(&info)) {
    cnt++;
    info_arr.push_back(info);
  }
  REQUIRE(cnt == 2);

  REQUIRE(utils::StartsWith(info_arr[0].PhraseEnds(), ".worker page") == true);
  REQUIRE(utils::StartsWith(info_arr[1].PhraseEnds(), "address commun.fifth.evid..core") == true);

  std::cout << info_arr[0].PhraseEnds() << std::endl;
  std::cout << info_arr[0].Tokens() << std::endl;

  auto tokens = info_arr[0].GetTokens();
  auto ends = info_arr[0].GetPhraseEnds();

  for (int i = 0; i < 10; i++) {
    std::cout << tokens[i] << "(" << ends[i] << ")" << std::endl;
  }

  REQUIRE(info_arr[0].GetPhraseEnds().size() == info_arr[0].GetTokens().size());
  REQUIRE(info_arr[1].GetPhraseEnds().size() == info_arr[1].GetTokens().size());
}


TEST_CASE( "Loading Engine with phrase end", "[engine]" ) {
  auto engine = CreateSearchEngine("qq_mem_compressed");
  REQUIRE(engine->TermCount() == 0);
  engine->LoadLocalDocuments("src/testdata/line_doc.with-bloom.toy", 10000, 
      "WITH_PHRASE_END");
  REQUIRE(engine->TermCount() > 0);
  auto result = engine->Search(SearchQuery({"prefix"}));
  std::cout << result.ToStr() << std::endl;
  REQUIRE(result.Size() > 0);

  {
    SearchQuery query({"solar", "body"}, true);
    result = engine->Search(query);
    std::cout << result.ToStr() << std::endl;
    REQUIRE(result.Size() == 0); // only solar is a valid term
  }
}


TEST_CASE( "Bloom filter store", "[bloomfilter]" ) {
  BloomFilterStore store(0.00001);
  store.Add(33, {"hello"}, {"world you"});
  FilterCases cases = store.Lookup("hello");
  
  REQUIRE(cases.size() == 1);
  REQUIRE(cases[0].doc_id == 33);

  REQUIRE(cases[0].blm.Check("world") == BLM_MAY_PRESENT);
  REQUIRE(cases[0].blm.Check("you") == BLM_MAY_PRESENT);
  REQUIRE(cases[0].blm.Check("yeu") == BLM_NOT_PRESENT);
  REQUIRE(cases[0].blm.Check("yew") == BLM_NOT_PRESENT);
}


