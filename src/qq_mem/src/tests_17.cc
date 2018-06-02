#include "catch.hpp"

#include <iostream>

#include "bloom_filter.h"


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

void CheckFloat(const float a) {
  std::string data = utils::SerializeFloat(a);
  float b = utils::DeserializeFloat(data.data());

  REQUIRE(a == b);
}

TEST_CASE( "Serializing float", "[utils]" ) {
  CheckFloat(0.1);
  CheckFloat(100.1);
  CheckFloat(100.103);
  CheckFloat(0.7803);
  CheckFloat(0.007803);
}







