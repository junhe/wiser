// #define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include <iostream>
#include <set>

#include <boost/filesystem.hpp>
#include <boost/lambda/lambda.hpp>    
#include "catch.hpp"
#include <glog/logging.h>

#include "native_doc_store.h"
#include "inverted_index.h"
#include "qq_engine.h"
#include "index_creator.h"
#include "utils.h"
#include "hash_benchmark.h"
#include "grpc_server_impl.h"

#include "posting_list_direct.h"
#include "posting_list_raw.h"
#include "posting_list_protobuf.h"
#include "posting_list_vec.h"

#include "intersect.h"

#include "unifiedhighlighter.h"

unsigned int Factorial( unsigned int number ) {
    return number <= 1 ? number : Factorial(number-1)*number;
}

TEST_CASE( "glog can print", "[glog]" ) {
  LOG(ERROR) << "Found " << 4 << " cookies in GLOG";
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

    // Get just the iterator
    {
      IndexStore::const_iterator ci = index.Find(Term("hello"));
      REQUIRE(index.ConstEnd() != ci);

      ci = index.Find(Term("hellox"));
      REQUIRE(index.ConstEnd() == ci);
    }

    // Get non-exist
    {
        std::vector<int> ids = index.GetDocumentIds("notexist");
        REQUIRE(ids.size() == 0);
    }

    // Get existing
    {
        std::vector<int> ids = index.GetDocumentIds("hello");
        REQUIRE(ids.size() == 2);
        
        std::set<int> s1(ids.begin(), ids.end());
        REQUIRE(s1 == std::set<int>{100, 101});
    }

    // Search "hello"
    {
        auto doc_ids = index.Search(TermList{"hello"}, SearchOperator::AND);
        REQUIRE(doc_ids.size() == 2);

        std::set<int> s(doc_ids.begin(), doc_ids.end());
        REQUIRE(s == std::set<int>{100, 101});
    }

    // Search "world"
    {
        auto doc_ids = index.Search(TermList{"earth"}, SearchOperator::AND);
        REQUIRE(doc_ids.size() == 1);

        std::set<int> s(doc_ids.begin(), doc_ids.end());
        REQUIRE(s == std::set<int>{101});
    }

    // Search non-existing
    {
        auto doc_ids = index.Search(TermList{"nonexist"}, SearchOperator::AND);
        REQUIRE(doc_ids.size() == 0);
    }
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

    for (auto it = pl.cbegin(); it != pl.cend(); it++) {
        auto p = pl.ExtractPosting(it); 
        REQUIRE((p.docID_ == 111 || p.docID_ == 232));
        if (p.docID_ == 111) {
            REQUIRE(p.term_frequency_ == 1);
            REQUIRE(p.positions_.size() == 1);
            REQUIRE(p.positions_[0] == 19);
        } else {
            // p.docID_ == 232
            REQUIRE(p.term_frequency_ == 2);
            REQUIRE(p.positions_.size() == 2);
            REQUIRE(p.positions_[0] == 10);
            REQUIRE(p.positions_[1] == 19);
        }
    }
}

TEST_CASE( "Raw String based Posting List essential operations are OK", "[posting_list_raw]" ) {
    PostingList_Raw pl("term001");
    REQUIRE(pl.Size() == 0);

    pl.AddPosting(111, 1,  Positions {19});
    pl.AddPosting(232, 2,  Positions {10, 19});

    REQUIRE(pl.Size() == 2);

    std::string serialized = pl.Serialize();
    PostingList_Raw pl1("term001", serialized);
    Posting p1 = pl1.GetPosting();
    REQUIRE(p1.docID_ == 111);
    REQUIRE(p1.term_frequency_ == 1);
    REQUIRE(p1.positions_.size() == 1);
    
    Posting p2 = pl1.GetPosting();
    REQUIRE(p2.docID_ == 232);
    REQUIRE(p2.term_frequency_ == 2);
    REQUIRE(p2.positions_.size() == 2);
}


TEST_CASE( "Protobuf based Posting List essential operations are OK", "[posting_list_protobuf]" ) {
    PostingList_Protobuf pl("term001");
    REQUIRE(pl.Size() == 0);

    pl.AddPosting(111, 1,  Positions {19});
    pl.AddPosting(232, 2,  Positions {10, 19});

    REQUIRE(pl.Size() == 2);

    std::string serialized = pl.Serialize();
    PostingList_Protobuf pl1("term001", serialized);
    Posting p1 = pl1.GetPosting();
    REQUIRE(p1.docID_ == 111);
    REQUIRE(p1.term_frequency_ == 1);
    REQUIRE(p1.positions_.size() == 1);
    
    Posting p2 = pl1.GetPosting();
    REQUIRE(p2.docID_ == 232);
    REQUIRE(p2.term_frequency_ == 2);
    REQUIRE(p2.positions_.size() == 2);
}

TEST_CASE( "QQSearchEngine", "[engine]" ) {
    QQSearchEngine engine;

    REQUIRE(engine.NextDocId() == 0);

    engine.AddDocument("my title", "my url", "my body");

    std::vector<int> doc_ids = engine.Search(TermList{"my"}, SearchOperator::AND);
    REQUIRE(doc_ids.size() == 1);

    std::string doc = engine.GetDocument(doc_ids[0]);
    REQUIRE(doc == "my body");
}


TEST_CASE( "QQSearchEngine can Find() a term", "[engine, benchmark]" ) {
    QQSearchEngine engine;

    engine.AddDocument("my title", "my url", "my body");

    InvertedIndex::const_iterator it = engine.Find(Term("my"));

    // We do not do any assertion here. 
    // Just want to make sure it runs. 
}



TEST_CASE( "Utilities", "[utils]" ) {
    SECTION("Leading space and Two spaces") {
        std::vector<std::string> vec = utils::explode(" hello  world", ' ');
        REQUIRE(vec.size() == 2);
        REQUIRE(vec[0] == "hello");
        REQUIRE(vec[1] == "world");
    }

    SECTION("Empty string") {
        std::vector<std::string> vec = utils::explode("", ' ');
        REQUIRE(vec.size() == 0);
    }

    SECTION("All spaces") {
        std::vector<std::string> vec = utils::explode("   ", ' ');
        REQUIRE(vec.size() == 0);
    }

    SECTION("Explode by some commone spaces") {
        std::vector<std::string> vec = utils::explode_by_non_letter(" hello\t world yeah");
        REQUIRE(vec.size() == 3);
        REQUIRE(vec[0] == "hello");
        REQUIRE(vec[1] == "world");
        REQUIRE(vec[2] == "yeah");
    }
}

TEST_CASE("Strict spliting", "[utils]") {
    SECTION("Regular case") {
        std::vector<std::string> vec = utils::explode_strict("a\tb", '\t');
        REQUIRE(vec.size() == 2);
        REQUIRE(vec[0] == "a");
        REQUIRE(vec[1] == "b");
    }

    SECTION("Separator at beginning and end of line") {
        std::vector<std::string> vec = utils::explode_strict("\ta\tb\t", '\t');

        REQUIRE(vec.size() == 4);
        REQUIRE(vec[0] == "");
        REQUIRE(vec[1] == "a");
        REQUIRE(vec[2] == "b");
        REQUIRE(vec[3] == "");
    }
}

TEST_CASE("Filling zeros", "[utils]") {
    REQUIRE(utils::fill_zeros("abc", 5) == "00abc");
    REQUIRE(utils::fill_zeros("abc", 0) == "abc");
    REQUIRE(utils::fill_zeros("abc", 3) == "abc");
    REQUIRE(utils::fill_zeros("", 3) == "000");
    REQUIRE(utils::fill_zeros("", 0) == "");
}

TEST_CASE( "LineDoc", "[line_doc]" ) {
    SECTION("Small size") {
        utils::LineDoc linedoc("src/testdata/tokenized_wiki_abstract_line_doc");
        std::vector<std::string> items;

        auto ret = linedoc.GetRow(items); 
        REQUIRE(ret == true);
        REQUIRE(items[0] == "col1");
        REQUIRE(items[1] == "col2");
        REQUIRE(items[2] == "col3");
        // for (auto s : items) {
            // std::cout << "-------------------" << std::endl;
            // std::cout << s << std::endl;
        // }
        
        ret = linedoc.GetRow(items);
        REQUIRE(ret == true);
        REQUIRE(items[0] == "Wikipedia: Anarchism");
        REQUIRE(items[1] == "Anarchism is a political philosophy that advocates self-governed societies based on voluntary institutions. These are often described as stateless societies,\"ANARCHISM, a social philosophy that rejects authoritarian government and maintains that voluntary institutions are best suited to express man's natural social tendencies.");
        REQUIRE(items[2] == "anarch polit philosophi advoc self govern societi base voluntari institut often describ stateless social reject authoritarian maintain best suit express man natur tendenc ");

        ret = linedoc.GetRow(items);
        REQUIRE(ret == false);
    }

    SECTION("Large size") {
        utils::LineDoc linedoc("src/testdata/test_doc_tokenized");
        std::vector<std::string> items;

        while (true) {
            std::vector<std::string> items;
            bool has_it = linedoc.GetRow(items);
            if (has_it) {
                REQUIRE(items.size() == 3);    
            } else {
                break;
            }
        }
    }
}

TEST_CASE( "boost library is usable", "[boost]" ) {
    using namespace boost::filesystem;

    path p{"./somefile-not-exist"};
    try
    {
        file_status s = status(p);
        REQUIRE(is_directory(s) == false);
        REQUIRE(exists(s) == false);
    }
    catch (filesystem_error &e)
    {
        std::cerr << e.what() << '\n';
    }
}

TEST_CASE( "Hash benchmark", "[benchmark]" ) {
    STLHash stl_hash;

    for (std::size_t i = 0; i < 100; i++) {
        stl_hash.Put(std::to_string(i), std::to_string(i * 100));
    }

    for (std::size_t i = 0; i < 100; i++) {
        std::string v = stl_hash.Get(std::to_string(i));
        REQUIRE(v == std::to_string(i * 100)); 
    }

    REQUIRE(stl_hash.Size() == 100);

}

TEST_CASE( "GRPC Sync Client and Server", "[grpc]" ) {
  auto server = CreateServer(std::string("localhost:50051"), 1, 1, 0);
  utils::sleep(1); // warm up the server

  auto client = CreateSyncClient("localhost:50051");

  SECTION("Echo") {
    EchoData request;
    request.set_message("hello");
    EchoData reply;
    auto ret = client->Echo(request, reply);

    REQUIRE(ret == true);
    REQUIRE(reply.message() == "hello");
    utils::sleep(1);

    // server will automatically destructed here.
  }

  SECTION("Add documents and search") {
    std::vector<int> doc_ids;
    bool ret;

    ret = client->Search("body", doc_ids);
    REQUIRE(doc_ids.size() == 0);
    REQUIRE(ret == true);

    client->AddDocument("my title", "my url", "my body");
    client->AddDocument("my title", "my url", "my spirit");

    doc_ids.clear();
    client->Search("body", doc_ids);
    REQUIRE(ret == true);
    REQUIRE(doc_ids.size() == 1);

    // server will automatically destructed here.
  }

}

void run_client() {
  auto client = CreateAsyncClient("localhost:50051", 64, 100, 1000, 8, 1, 8);
  client->Wait();
}

TEST_CASE( "GRPC Async Client and Server", "[grpc]" ) {
  auto server = CreateServer(std::string("localhost:50051"), 1, 40, 0);
  utils::sleep(1); // warm up the server

  // Use another thread to create client
  // std::thread client_thread(run_client);
  auto client = CreateAsyncClient("localhost:50051", 64, 100, 1000, 8, 1, 2);
  client->Wait();
  // client->ShowStats();
  client.release();

  // client_thread.join();
}

TEST_CASE( "IndexCreator works over the network", "[grpc]" ) {
  auto server = CreateServer(std::string("localhost:50051"), 1, 40, 0);
  utils::sleep(1); // warm up the server

  auto client = CreateSyncClient("localhost:50051");

  IndexCreator index_creator(
        "src/testdata/enwiki-abstract_tokenized.linedoc.sample", *client);
  index_creator.DoIndex();

  // Search synchroniously
  std::vector<int> doc_ids;
  bool ret;
  ret = client->Search("multicellular", doc_ids);
  REQUIRE(ret == true);
  REQUIRE(doc_ids.size() == 1);

  // client_thread.join();
}


TEST_CASE( "Search engine can load document and index them locally", "[engine]" ) {
  QQSearchEngine engine;
  int ret = engine.LoadLocalDocuments("src/testdata/enwiki-abstract_tokenized.linedoc.sample", 90);
  REQUIRE(ret == 90);

  std::vector<int> doc_ids = engine.Search(TermList{"multicellular"}, SearchOperator::AND);
  REQUIRE(doc_ids.size() == 1);
}

TEST_CASE( "Unified Highlighter essential operations are OK", "[unified_highlighter]" ) {
    QQSearchEngine engine;
    engine.AddDocument("my title", "my url", "hello world. This is Kan ...    Hello Kan, This is Madison!");
    /*engine.AddDocument("my title", "my url", "hello earth\n This is Kan! hello world. This is Kan!");
    engine.AddDocument("my title", "my url", "hello Madison.... This is Kan. hello world. This is Kan!");
    engine.AddDocument("my title", "my url", "hello Wisconsin, This is Kan. Im happy. hello world. This is Kan!");
*/
    UnifiedHighlighter test_highlighter(engine);
    Query query = {"hello", "kan"};
    TopDocs topDocs = {0}; 

    int maxPassages = 3;
    std::vector<std::string> res = test_highlighter.highlight(query, topDocs, maxPassages);
    REQUIRE(res.size()  == topDocs.size());
    /*REQUIRE(res[0] == "hello world");
    REQUIRE(res[1] == "hello earth");
    REQUIRE(res[2] == "hello Wisconsin");
    */
}

TEST_CASE( "SentenceBreakIterator essential operations are OK", "[sentence_breakiterator]" ) {

    // init 
    std::string test_string = "hello Wisconsin, This is Kan.  Im happy.";
    int breakpoint[2] = {30, 39};
    SentenceBreakIterator breakiterator(test_string);

    // iterate on a string
    int i = 0;
    while ( breakiterator.next() > 0 ) {
        
        int start_offset = breakiterator.getStartOffset();
        int end_offset = breakiterator.getEndOffset();
        REQUIRE(end_offset == breakpoint[i++]);
    }   

    // test offset-based next
    REQUIRE(i == 2);
    breakiterator.next(3);
    REQUIRE(breakiterator.getEndOffset() == 30);
    breakiterator.next(33);
    REQUIRE(breakiterator.getEndOffset() == 39);
    REQUIRE(breakiterator.next(40) == 0);
}


TEST_CASE( "Vector-based posting list works fine", "[posting_list]" ) {
  SECTION("New postings can be added and retrieved") {
    PostingList_Vec<Posting> pl("hello");   

    REQUIRE(pl.Size() == 0);
    pl.AddPosting(Posting(10, 88, Positions{28}));
    REQUIRE(pl.Size() == 1);
    REQUIRE(pl.GetPosting(0).GetDocId() == 10);
    REQUIRE(pl.GetPosting(0).GetTermFreq() == 88);
    REQUIRE(pl.GetPosting(0).GetPositions() == Positions{28});

    pl.AddPosting(Posting(11, 889, Positions{28, 230}));
    REQUIRE(pl.Size() == 2);
    REQUIRE(pl.GetPosting(1).GetDocId() == 11);
    REQUIRE(pl.GetPosting(1).GetTermFreq() == 889);
    REQUIRE(pl.GetPosting(1).GetPositions() == Positions{28, 230});
  }


  SECTION("Skipping works") {
    PostingList_Vec<Posting> pl("hello");   
    for (int i = 0; i < 100; i++) {
      pl.AddPosting(Posting(i, 1, Positions{28}));
    }
    REQUIRE(pl.Size() == 100);

    SECTION("It can stay at starting it") {
      PostingList_Vec<Posting>::iterator_t it;
      it = pl.SkipForward(0, 0);
      REQUIRE(it == 0);

      it = pl.SkipForward(8, 8);
      REQUIRE(it == 8);
    }

    SECTION("It can skip multiple postings") {
      PostingList_Vec<Posting>::iterator_t it;
      it = pl.SkipForward(0, 8);
      REQUIRE(it == 8);

      it = pl.SkipForward(0, 99);
      REQUIRE(it == 99);
    }

    SECTION("It returns pl.Size() when we cannot find doc id") {
      PostingList_Vec<Posting>::iterator_t it;
      it = pl.SkipForward(0, 1000);
      REQUIRE(it == pl.Size());
    }
  }

}


TEST_CASE( "Intersection", "[intersect]" ) {
  PostingList_Vec<Posting> pl01("hello");   
  for (int i = 0; i < 10; i++) {
    pl01.AddPosting(Posting(i, 1, Positions{28}));
  }

  SECTION("It intersects a subset") {
    PostingList_Vec<Posting> pl02("world");   
    for (int i = 5; i < 10; i++) {
      pl02.AddPosting(Posting(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect(lists);
    REQUIRE(ret == std::vector<DocIdType>{5, 6, 7, 8, 9});
  }

  SECTION("It intersects an empty posting list") {
    PostingList_Vec<Posting> pl02("world");   

    std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect(lists);
    REQUIRE(ret == std::vector<DocIdType>{});
  }

  SECTION("It intersects a non-overlapping posting list") {
    PostingList_Vec<Posting> pl02("world");   
    for (int i = 10; i < 20; i++) {
      pl02.AddPosting(Posting(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect(lists);
    REQUIRE(ret == std::vector<DocIdType>{});
  }

  SECTION("It intersects a partial-overlapping posting list") {
    PostingList_Vec<Posting> pl02("world");   
    for (int i = 5; i < 15; i++) {
      pl02.AddPosting(Posting(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect(lists);
    REQUIRE(ret == std::vector<DocIdType>{5, 6, 7, 8, 9});
  }

  SECTION("It intersects a super set") {
    PostingList_Vec<Posting> pl02("world");   
    for (int i = 0; i < 15; i++) {
      pl02.AddPosting(Posting(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect(lists);
    REQUIRE(ret == std::vector<DocIdType>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  }

  SECTION("It intersects two empty list") {
    PostingList_Vec<Posting> pl01("hello");   
    PostingList_Vec<Posting> pl02("world");   

    std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect(lists);
    REQUIRE(ret == std::vector<DocIdType>{});
  }
}

