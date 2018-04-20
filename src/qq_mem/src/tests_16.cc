#include "catch.hpp"

#include "query_pool.h"

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
}

