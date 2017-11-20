// #define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch.hpp"
#include "native_doc_store.h"
#include "inverted_index.h"
#include "posting_list.h"

#include <set>

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
    index.AddDocument(100, TermList{"hello", "world"});
    index.AddDocument(101, TermList{"hello", "earth"});

    {
        std::vector<int> ids = index.GetDocumentIds("notexist");
        REQUIRE(ids.size() == 0);
    }

    {
        std::vector<int> ids = index.GetDocumentIds("hello");
        REQUIRE(ids.size() == 2);
        
        std::set<int> s1(ids.begin(), ids.end());
        REQUIRE(s1 == std::set<int>{100, 101});
    }
}

TEST_CASE( "Posting", "[posting_list]" ) {
    Posting posting(100, 200, Positions {1, 2});
    REQUIRE(posting.docID_ == 100);
    REQUIRE(posting.term_frequency_ == 200);
    REQUIRE(posting.positions_[0] == 1);
    REQUIRE(posting.positions_[1] == 2);
}

TEST_CASE( "Posting List essential operations are OK", "[posting_list]" ) {
    PostingList pl("term001");
    REQUIRE(pl.Size() == 0);

    pl.AddPosting(111, 1,  Positions {19});
    REQUIRE(pl.Size() == 1);

    REQUIRE(pl.GetPosting(111).docID_ == 111);
    REQUIRE(pl.GetPosting(111).term_frequency_ == 1);
    REQUIRE(pl.GetPosting(111).positions_.size() == 1);
}


