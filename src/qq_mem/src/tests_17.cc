#include "catch.hpp"

#include <iostream>

#include "bloom_filter.h"


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






