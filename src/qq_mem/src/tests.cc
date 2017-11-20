// #define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch.hpp"
#include "native_doc_store.h"
#include "inverted_index.h"
#include "posting_list_direct.h"
#include "posting_list_raw.h"

unsigned int Factorial( unsigned int number ) {
    return number <= 1 ? number : Factorial(number-1)*number;
}

TEST_CASE( "Factorials are computed", "[factorial]" ) {
    REQUIRE( Factorial(1) == 1 );
    REQUIRE( Factorial(2) == 2 );
    REQUIRE( Factorial(3) == 6 );
    REQUIRE( Factorial(10) == 3628800 );
}

TEST_CASE( "Document store implemented by C++ map", "[docstore]" ) {
    NativeDocStore store;
    int doc_id = 88;
    std::string doc = "it is a doc";

    REQUIRE(store.Has(doc_id) == false);

    store.Add(doc_id, doc);
    REQUIRE(store.Get(doc_id) == doc);

    store.Add(89, "doc89");
    REQUIRE(store.Size() == 2);

    store.Remove(89);
    REQUIRE(store.Size() == 1);

    REQUIRE(store.Has(doc_id) == true);

    store.Clear();
    REQUIRE(store.Has(doc_id) == false);
}

TEST_CASE( "Inverted Index essential operations are OK", "[inverted_index]" ) {
    InvertedIndex index;     
}

TEST_CASE( "Basic Posting", "[posting_list]" ) {
    Posting posting(100, 200, Positions {1, 2});
    REQUIRE(posting.docID_ == 100);
    REQUIRE(posting.term_frequency_ == 200);
    REQUIRE(posting.positions_[0] == 1);
    REQUIRE(posting.positions_[1] == 2);
}

TEST_CASE( "Direct Posting List essential operations are OK", "[posting_list_direct]" ) {
    PostingList_Direct pl("term001");
    REQUIRE(pl.Size() == 0);

    pl.AddPosting(111, 1,  Positions {19});
    pl.AddPosting(232, 2,  Positions {10, 19});

    REQUIRE(pl.Size() == 2);

    Posting p1 = pl.GetPosting();
    REQUIRE(p1.docID_ == 111);
    REQUIRE(p1.term_frequency_ == 1);
    REQUIRE(p1.positions_.size() == 1);

    Posting p2 = pl.GetPosting();
    REQUIRE(p2.docID_ == 232);
    REQUIRE(p2.term_frequency_ == 2);
    REQUIRE(p2.positions_.size() == 2);
}

TEST_CASE( "Raw String based Posting List essential operations are OK", "[posting_list_raw]" ) {
    PostingList_Raw pl("term001");
    REQUIRE(pl.Size() == 0);

    pl.AddPosting(111, 1,  Positions {19});
    pl.AddPosting(232, 2,  Positions {10, 19});

    REQUIRE(pl.Size() == 2);

    Posting p1 = pl.GetPosting();
    REQUIRE(p1.docID_ == 111);
    REQUIRE(p1.term_frequency_ == 1);
    REQUIRE(p1.positions_.size() == 1);
/*
    Posting p2 = pl.GetPosting();
    REQUIRE(p2.docID_ == 232);
    REQUIRE(p2.term_frequency_ == 2);
    REQUIRE(p2.positions_.size() == 2);
*/
}


