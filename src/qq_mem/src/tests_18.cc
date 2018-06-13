#include "catch.hpp"

#include <iostream>

#include "test_helpers.h"
#include "bloom_filter.h"
#include "flash_containers.h"
#include "engine_factory.h"


void CheckSkipList(int n) {
  std::vector<off_t> offs;
  for (int i = 0; i < n; i++) {
    offs.push_back(PsudoIncreasingRandom(i));
  }

  BloomSkipListWriter writer(offs);
  std::string data = writer.Serialize();

  REQUIRE(data[0] == (char) BLOOM_SKIP_LIST_FIRST_BYTE);

  BloomSkipList skiplist;
  skiplist.Load((const uint8_t *)data.data());
  REQUIRE(skiplist.Size() == offs.size());

  for (int i = 0; i < n; i++) {
    REQUIRE(skiplist[i] == offs[i]);
  }
}


TEST_CASE( "Bloom box skip list", "[bloomfilter]" ) {
  SECTION("Simple") {
    CheckSkipList(0);
    CheckSkipList(1);
    CheckSkipList(10);
    CheckSkipList(100);
  }

  SECTION("Large offsets") {
    std::vector<off_t> offs = {4*GB, 5*GB, 10*GB};

    BloomSkipListWriter writer(offs);
    std::string data = writer.Serialize();

    REQUIRE(data[0] == (char) BLOOM_SKIP_LIST_FIRST_BYTE);

    std::cout << "large offsets" << std::endl;
    BloomSkipList skiplist;
    skiplist.Load((const uint8_t *)data.data());
    REQUIRE(skiplist.Size() == offs.size());

    for (std::size_t i = 0; i < offs.size(); i++) {
      REQUIRE(skiplist[i] == offs[i]);
    }
  }
}


TEST_CASE( "Varint large number", "[varint]" ) {
  VarintBuffer buf;
  off_t off = 5 * GB;
  buf.Append(off);

  std::string data = buf.Data();
  VarintIterator it(buf, 1);

  off_t off2 = it.Pop();
  REQUIRE(off == off2);

}


std::string GenBitarray(int i, int array_bytes) {
  if (i % 5 == 0) 
    return "";
  else
    return utils::fill_zeros(std::to_string(i), array_bytes);
}

void CheckBloomColumn(const int n_items) {
  const int array_bytes = 4;
  // Write the column
  BloomFilterColumnWriter writer(array_bytes);

  for (int i = 0; i < n_items; i++) {
    writer.AddPostingBag(GenBitarray(i, array_bytes));
  }

  FileDumper file_dumper("/tmp/temp.dumper");
  std::vector<off_t> offs = writer.Dump(&file_dumper);
  file_dumper.Flush();
  file_dumper.Close();

  // Get skip list
  BloomSkipListWriter skip_list_writer(offs);
  std::string skip_list_data = skip_list_writer.Serialize();

  // Read it back
  utils::FileMap file_map;
  file_map.Open("/tmp/temp.dumper");

  BloomFilterColumnReader reader;
  reader.Reset(
      (const uint8_t *)file_map.Addr(), 
      (const uint8_t *)skip_list_data.data(), 
      array_bytes);
  reader.LoadSkipList();

  for (int i = 0; i < n_items; i++) {
    reader.SkipTo(i);
    std::string array_in_file;
    if (reader.BitArray() == nullptr) {
      array_in_file = "";
    } else {
      array_in_file = std::string(
          (const char *)reader.BitArray(), array_bytes);
    }

    REQUIRE(GenBitarray(i, array_bytes) == array_in_file);
  }

  file_map.Close();
}

TEST_CASE( "Blooom filter column writer", "[bloomfilter]" ) {
  CheckBloomColumn(1);
  CheckBloomColumn(3);
  CheckBloomColumn(128);
  CheckBloomColumn(129);
  CheckBloomColumn(300);
}


TEST_CASE( "Get Serialized offsetes", "[bloomfilter]" ) {
  const int array_bytes = 4;
  // Write the column
  BloomFilterColumnWriter writer(array_bytes);

  for (int i = 0; i < 300; i++) {
    writer.AddPostingBag(GenBitarray(i, array_bytes));
  }

  FileDumper file_dumper("/tmp/temp.dumper");
  std::vector<off_t> offs = writer.Dump(&file_dumper);
  file_dumper.Flush();
  file_dumper.Close();

  std::vector<off_t> offs2 = GetSerializedOffsets(writer);

  REQUIRE(offs == offs2);
}

// Return a list of phrase that exist in the phrase file
std::vector<TermList> GetPhrases() {
  std::vector<TermList> phrases;

  std::vector<std::string> lines = utils::ReadLines(
      "src/testdata/line_doc.with-bloom.toy-phrases");

  for (auto &line : lines) {
    std::vector<std::string> items = utils::explode(line, ':');
    std::string first = items[0];

    if (items.size() > 1) {
      std::vector<std::string> ends = utils::explode(items[1], ' ');      
      for (auto &end : ends) {
        TermList phrase = {first, end};
        phrases.push_back(phrase);
      }
    }
  }

  return phrases;
}


TEST_CASE( "Loading Engine with phrase begin and ends", "[engine]" ) {
  auto engine = CreateSearchEngine("qq_mem_compressed");
  REQUIRE(engine->TermCount() == 0);
  engine->LoadLocalDocuments("src/testdata/wiki_linedoc.toy.pre-suf-bloom", 
      10000, 
      "WITH_BI_BLOOM");
  REQUIRE(engine->TermCount() > 0);
  auto result = engine->Search(SearchQuery({"prefix"}));
  std::cout << result.ToStr() << std::endl;
  REQUIRE(result.Size() > 0);

  SECTION("Trivial test") {
    SearchQuery query({"solar", "body"}, true);
    result = engine->Search(query);
    std::cout << result.ToStr() << std::endl;
    REQUIRE(result.Size() == 0); // only solar is a valid term
  }

  SECTION("Serialize it") {
    utils::RemoveDir("/tmp/bloom-qq-engine-tmp");
    utils::RemoveDir("/tmp/bloom-vacuum-engine-tmp");

    // Dump to qq mem format
    engine->Serialize("/tmp/bloom-qq-engine-tmp");

    // Dump to vacuum format
    utils::PrepareDir("/tmp/bloom-vacuum-engine-tmp");
    FlashEngineDumper engine_dumper("/tmp/bloom-vacuum-engine-tmp", true);
    engine_dumper.LoadQqMemDump("/tmp/bloom-qq-engine-tmp");
    engine_dumper.Dump();

    SECTION("Load to Vacuum") {
      auto vac_engine = CreateSearchEngine(
          "vacuum:vacuum_dump:/tmp/bloom-vacuum-engine-tmp", 1);
      vac_engine->Load();
      SearchQuery query({"close"});
      result = vac_engine->Search(query);
      std::cout << result.ToStr() << std::endl;
      REQUIRE(result.Size() > 0); // only solar is a valid term

      SECTION("Query existing phrases") {
        // Must find these phrases
        std::vector<TermList> phrases = GetPhrases();
        for (auto &phrase : phrases) {
          SearchQuery query(phrase);
          query.is_phrase = true;

          std::cout << query.ToStr() << std::endl;

          result = vac_engine->Search(query);
          std::cout << result.ToStr() << std::endl;
          REQUIRE(result.Size() > 0); // only solar is a valid term
        }
      }

      SECTION("Query non-existing phrases") {
        // Must find these phrases
        std::vector<TermList> phrases = GetPhrases();
        for (auto &phrase : phrases) {
          phrase[1] = "xxyxz3";
          SearchQuery query(phrase);
          query.is_phrase = true;

          std::cout << query.ToStr() << std::endl;

          result = vac_engine->Search(query);
          std::cout << result.ToStr() << std::endl;
          REQUIRE(result.Size() == 0); // only solar is a valid term
        }
      }
    }
  }
}


TEST_CASE( "Bloom filter store with empty phrase ends", "[bloomfilter]" ) {
  BloomFilterStore store(0.00001, 5);
  store.Add(33, {"hello"}, {"world"});
  BloomFilterCases cases = store.Lookup("hello");
  
  BloomFilterStore store2(0.00001, 5);
  store2.Add(33, {"hello"}, {""});
  BloomFilterCases cases2 = store2.Lookup("hello");
   
  REQUIRE(cases.Serialize().size() > cases2.Serialize().size());
}


TEST_CASE( "Write and read file", "[utils]" ) {
  utils::AppendLines("/tmp/tmpout", {"hello1", "hello2"});
  utils::AppendLines("/tmp/tmpout", {"hello3", "hello4"});
  auto lines = utils::ReadLines("/tmp/tmpout");
  REQUIRE(lines[0] == "hello1");
  REQUIRE(lines[1] == "hello2");
  REQUIRE(lines[2] == "hello3");
  REQUIRE(lines[3] == "hello4");
}



TEST_CASE( "Loading Engine with bi-bloom (3 docs)", "[blmxx]" ) {
  auto engine = CreateSearchEngine("qq_mem_compressed");
  REQUIRE(engine->TermCount() == 0);
  engine->LoadLocalDocuments("src/testdata/iter_test_3_docs_tf_bi-bloom",
      10000, "WITH_BI_BLOOM");
  REQUIRE(engine->TermCount() > 0);
  auto result = engine->Search(SearchQuery({"a"}));
  std::cout << result.ToStr() << std::endl;
  REQUIRE(result.Size() > 0);

  SECTION("Serialize it") {
    utils::RemoveDir("/tmp/bloom-qq-engine-tmp");
    utils::RemoveDir("/tmp/bloom-vacuum-engine-tmp");

    // Dump to qq mem format
    engine->Serialize("/tmp/bloom-qq-engine-tmp");

    // Dump to vacuum format
    utils::PrepareDir("/tmp/bloom-vacuum-engine-tmp");
    FlashEngineDumper engine_dumper("/tmp/bloom-vacuum-engine-tmp", true, true);
    engine_dumper.LoadQqMemDump("/tmp/bloom-qq-engine-tmp");
    engine_dumper.Dump();

    SECTION("Load to Vacuum") {
      auto vac_engine = CreateSearchEngine(
          "vacuum:vacuum_dump:/tmp/bloom-vacuum-engine-tmp", 1);
      vac_engine->Load();
      {
        SearchQuery query({"a"});
        result = vac_engine->Search(query);
        std::cout << result.ToStr() << std::endl;
        REQUIRE(result.Size() > 0); // only solar is a valid term
      }
      {
        SearchQuery query({"z"});
        result = vac_engine->Search(query);
        std::cout << result.ToStr() << std::endl;
        REQUIRE(result.Size() == 0); // only solar is a valid term
      }

      // Must find these phrases
      std::vector<TermList> phrases = {{"a", "b"}, {"b", "c"}};
      for (auto &phrase : phrases) {
        SearchQuery query(phrase);
        query.is_phrase = true;

        std::cout << query.ToStr() << std::endl;

        result = vac_engine->Search(query);
        std::cout << result.ToStr() << std::endl;
        REQUIRE(result.Size() > 0); 
      }

      std::vector<TermList> non_phrases = {{"a", "x"}, {"x", "c"}};
      for (auto &phrase : non_phrases) {
        SearchQuery query(phrase);
        query.is_phrase = true;

        std::cout << query.ToStr() << std::endl;

        result = vac_engine->Search(query);
        std::cout << result.ToStr() << std::endl;
        REQUIRE(result.Size() == 0); 
      }
  
    }
  }
}



