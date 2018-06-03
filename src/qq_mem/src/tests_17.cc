#include "catch.hpp"

#include <iostream>

#include "bloom_filter.h"


typedef std::pair<std::set<std::string>, std::set<std::string>> set_pair ;
typedef std::pair<std::string, std::string> string_pair;

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


std::string Concat(std::set<std::string> set) {
  std::string ret;

  std::size_t i = 0;
  for (auto &s : set) {
    ret += s;

    if (i != (set.size() - 1)) {
      ret += " "; 
    }

    i++;
  }

  return ret;
}


string_pair GetRandSetStr(std::size_t n) {
  std::set<std::string> in, out;
  
  while (in.size() < n) {
    in.insert(std::to_string(rand()));
  }

  while (out.size() < n) {
    std::string s = std::to_string(rand());
    if (out.count(s) == 0)
      out.insert(s);
  }

  return string_pair(Concat(in), Concat(out));
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


TEST_CASE( "Bloom filter store", "[bloomfilter]" ) {
  SECTION("Bloom filter check") {
    BloomFilterStore store(0.00001);
    store.Add(33, {"hello"}, {"world you"});
    BloomFilterCases cases = store.Lookup("hello");
    
    REQUIRE(cases.Size() == 1);
    REQUIRE(cases[0].doc_id == 33);

    REQUIRE(cases[0].blm.Check("world") == BLM_MAY_PRESENT);
    REQUIRE(cases[0].blm.Check("you") == BLM_MAY_PRESENT);
    REQUIRE(cases[0].blm.Check("yeu") == BLM_NOT_PRESENT);
    REQUIRE(cases[0].blm.Check("yew") == BLM_NOT_PRESENT);

    BloomFilterCase f_case = cases[0];

    SECTION("Serialize and Deserialize") {
      std::string data = f_case.blm.Serialize();

      BloomFilter blm;
      blm.Deserialize(data.data());

      REQUIRE(f_case.blm.BitArray() == blm.BitArray());

      REQUIRE(blm.Check("world") == BLM_MAY_PRESENT);
      REQUIRE(blm.Check("you") == BLM_MAY_PRESENT);
      REQUIRE(blm.Check("yeu") == BLM_NOT_PRESENT);
      REQUIRE(blm.Check("yew") == BLM_NOT_PRESENT);
    }

    SECTION("Serialize and Deserialize _case_") {
      std::string data = f_case.Serialize();

      BloomFilterCase filter_case;
      filter_case.Deserialize(data.data());

      BloomFilter &blm = filter_case.blm;

      REQUIRE(blm.BitArray() == f_case.blm.BitArray());

      REQUIRE(blm.Check("world") == BLM_MAY_PRESENT);
      REQUIRE(blm.Check("you") == BLM_MAY_PRESENT);
      REQUIRE(blm.Check("yeu") == BLM_NOT_PRESENT);
      REQUIRE(blm.Check("yew") == BLM_NOT_PRESENT);
    }

    SECTION("Serialize/Deserialize cases") {
      std::string data = cases.Serialize();

      BloomFilterCases new_cases;
      new_cases.Deserialize(data.data());

      const BloomFilter &blm = new_cases[0].blm;

      REQUIRE(blm.BitArray() == f_case.blm.BitArray());

      REQUIRE(blm.Check("world") == BLM_MAY_PRESENT);
      REQUIRE(blm.Check("you") == BLM_MAY_PRESENT);
      REQUIRE(blm.Check("yeu") == BLM_NOT_PRESENT);
      REQUIRE(blm.Check("yew") == BLM_NOT_PRESENT);
    }

    SECTION("Serialize/Deserialize store") {
      store.Serialize("/tmp/filter.store");

      BloomFilterStore store2(0.00001);
      store2.Deserialize("/tmp/filter.store");
      BloomFilterCases cases = store2.Lookup("hello");
      
      REQUIRE(cases.Size() == 1);
      REQUIRE(cases[0].doc_id == 33);

      REQUIRE(cases[0].blm.Check("world") == BLM_MAY_PRESENT);
      REQUIRE(cases[0].blm.Check("you") == BLM_MAY_PRESENT);
      REQUIRE(cases[0].blm.Check("yeu") == BLM_NOT_PRESENT);
      REQUIRE(cases[0].blm.Check("yew") == BLM_NOT_PRESENT);
    }
  }
}


struct Row {
  Row(std::string t, std::string p, std::string o)
    :token(t), phrase_ends(p), outs(o) {}

  std::string ToStr() const {
    return "token: " + token + "\n" + \
      "ends: " + phrase_ends + "\n" + \
      "outs: " + outs + "\n";
  }

  std::string token;
  std::string phrase_ends;
  std::string outs;
};

struct Table {
  std::vector<Row> rows;
  
  std::vector<std::string> ColTokens() {
    std::vector<std::string> tokens;
    for (auto &row : rows) {
      tokens.push_back(row.token);
    }

    return tokens;
  }

  std::vector<std::string> ColEnds() {
    std::vector<std::string> vec;
    for (auto &row : rows) {
      vec.push_back(row.phrase_ends);
    }

    return vec;
  }

  std::vector<std::string> ColOuts() {
    std::vector<std::string> vec;
    for (auto &row : rows) {
      vec.push_back(row.outs);
    }

    return vec;
  }
};

void CheckStore(BloomFilterStore &store, Table &table) {
  for (auto &row : table.rows) {
    BloomFilterCases cases = store.Lookup(row.token); 
    REQUIRE(cases[0].doc_id == 888);
    if (cases.Size() == 2) {
      REQUIRE(cases[0].doc_id == 888);
    }

    for (std::size_t i = 0; i < cases.Size(); i++) {
      // Check real ends
      std::vector<std::string> real_ends = utils::explode(
          row.phrase_ends, ' ');
      for (auto &real_end : real_ends) {
        REQUIRE(cases[i].blm.Check(real_end) == BLM_MAY_PRESENT);
      }

      // check terms that are not in filter
      std::vector<std::string> outs = utils::explode(
          row.outs, ' ');
      for (auto &out : outs) {
        REQUIRE(cases[i].blm.Check(out) == BLM_NOT_PRESENT);
      }
    }
  }
}

TEST_CASE( "Bloom filter serialization", "[bloomfilter]" ) {
  Table table;

  for (int i = 0; i < 10; i++) {
    string_pair pair = GetRandSetStr(i);
    Row row(std::to_string(i), pair.first, pair.second); 
    table.rows.push_back(row);
  }

  SECTION("All tokens belong to the same the doc") {
    BloomFilterStore store(0.00001);
    
    store.Add(888, table.ColTokens(), table.ColEnds());

    CheckStore(store, table);

    store.Serialize("/tmp/tmp.store");

    BloomFilterStore store2(0.00001);
    store2.Deserialize("/tmp/tmp.store");

    CheckStore(store2, table);
  }

  SECTION("Each token has two documents") {
    BloomFilterStore store(0.00001);
    
    store.Add(888, table.ColTokens(), table.ColEnds());
    store.Add(889, table.ColTokens(), table.ColEnds());

    CheckStore(store, table);

    store.Serialize("/tmp/tmp.store");

    BloomFilterStore store2(0.00001);
    store2.Deserialize("/tmp/tmp.store");

    CheckStore(store2, table);
  }

}


TEST_CASE( "Set bits", "[utils]" ) {
  uint64_t val = 0;
  utils::SetBit(&val, 0);
  REQUIRE(val == 0x1);

  val = 0;
  utils::SetBit(&val, 63);
  REQUIRE(val == ((uint64_t) 1 << 63));
  utils::SetBit(&val, 62);
  REQUIRE(val == (((uint64_t) 1 << 63) | ((uint64_t) 1 << 62)));

  val = 0;
  utils::SetBitReverse(&val, 0);
  REQUIRE(val == ((uint64_t) 1 << 63));

  val = 0;
  utils::SetBitReverse(&val, 63);
  REQUIRE(val == ((uint64_t) 1 << 0));

  for (int i = 0; i < 64; i++) {
    utils::SetBitReverse(&val, i);
  }
  REQUIRE(val == ~((uint64_t) 0x00));
}


TEST_CASE( "Bloom box writer", "[flash]" ) {


}


