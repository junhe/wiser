// #define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include <sys/time.h>
#include <iostream>
#include <iomanip>
#include <set>
#include <sstream>

#include <boost/filesystem.hpp>
#include <boost/lambda/lambda.hpp>    
#include "catch.hpp"
#include <glog/logging.h>

#include "posting_message.pb.h"

#include "doc_store.h"
#include "utils.h"
#include "hash_benchmark.h"
#include "grpc_server_impl.h"
#include "qq_mem_engine.h"

#include "posting_list_vec.h"

#include "intersect.h"
#include "scoring.h"

#include "highlighter.h"

#include "lrucache.h"

// cereal
#include <cereal/archives/binary.hpp>


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
    SimpleDocStore store;
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

TEST_CASE( "Offsets Parser essential operations are OK", "[offsets_parser]" ) {
    std::string offsets = "1,2;.3,4;5,6;.7,8;.";
    std::vector<Offsets> offset_parsed = utils::parse_offsets(offsets);
    REQUIRE(offset_parsed.size() == 3);
    REQUIRE(offset_parsed[0].size() == 1);
    REQUIRE(offset_parsed[1].size() == 2);
    REQUIRE(offset_parsed[2].size() == 1);
    REQUIRE(offset_parsed[0][0]  == std::make_tuple(1,2));
    REQUIRE(offset_parsed[1][0]  == std::make_tuple(3,4));
    REQUIRE(offset_parsed[1][1]  == std::make_tuple(5,6));
    REQUIRE(offset_parsed[2][0]  == std::make_tuple(7,8));
}

TEST_CASE( "Passage of Unified Highlighter essential operations are OK", "[passage_unified_highlighter]" ) {
    // construct a passage
    Passage test_passage;
    test_passage.startoffset = 0;
    test_passage.endoffset = 10;

    // add matches
    test_passage.addMatch(0, 4);
    test_passage.addMatch(6, 10);
    REQUIRE(test_passage.matches.size() == 2);
    REQUIRE(test_passage.matches[0] == std::make_tuple(0,4));
    REQUIRE(test_passage.matches[1] == std::make_tuple(6,10));

    // to_string
    std::string doc = "Hello world. This is Kan.";
    std::string res = test_passage.to_string(&doc);
    // check 
    REQUIRE(res == "<b>Hello<\\b> <b>world<\\b>\n");

    // reset
    test_passage.reset();
    REQUIRE(test_passage.matches.size() == 0);
}

TEST_CASE( "SentenceBreakIterator essential operations are OK", "[sentence_breakiterator]" ) {

    // init 
    std::string test_string = "hello Wisconsin, This is Kan.  Im happy.";
    int breakpoint[2] = {30, 39};
    SentenceBreakIterator breakiterator(test_string, create_locale());

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


TEST_CASE( "LRUCache Template essential put/get opeartions are OK", "[put_get_LRUCache]") {
    // create cache
    cache::lru_cache<int, int> cache_lru(1);

    // put/get test
    cache_lru.put(7, 777);
    REQUIRE(cache_lru.exists(7) == true);
    REQUIRE(777 == cache_lru.get(7));
    REQUIRE(1 == cache_lru.size());

    // miss
    REQUIRE(cache_lru.exists(17) == false);
    REQUIRE_THROWS_AS(cache_lru.get(17), std::range_error);
}

TEST_CASE( "LRUCache Template essential Capacity limited  opeartions are OK", "[capacity_LRUCache]") {
    // create cache
    cache::lru_cache<int, int> cache_lru(10);

    for (int i = 0; i < 20; ++i) {
        cache_lru.put(i, i);
    }

    for (int i = 0; i < 10; ++i) {
        REQUIRE(cache_lru.exists(i) == false);
    }

    for (int i = 10; i < 20; ++i) {
        REQUIRE(cache_lru.exists(i) == true);
        REQUIRE(i == cache_lru.get(i));
    }

    REQUIRE(cache_lru.size() == 10);
}

TEST_CASE( "Flash based LRUCache Template essential put/get opeartions are OK", "[put_get_LRUCache_Flash]") {
    // create cache
    cache::lru_flash_cache<int, std::string> cache_lru(1, "src/testdata/flash_cache_test");

    // put/get test
    cache_lru.put(7, "hello world");
    REQUIRE(cache_lru.exists(7) == true);
    std::string s("hello world");
    //REQUIRE( cache_lru.get(7).compare(s) == 0);
    REQUIRE(1 == cache_lru.size());

    // miss
    REQUIRE(cache_lru.exists(17) == false);
    REQUIRE_THROWS_AS(cache_lru.get(17), std::range_error);
}

TEST_CASE("Precompute and store document sentence segments successfully", "[Precompute_StorePassages]") {
    SimpleDocStore store;
    int doc_id = 88;
    std::string doc = "it is a doc. We are the second sentence. I'm the third sentence! ";
    store.Add(doc_id, doc);
    
    if (FLAG_SNIPPETS_PRECOMPUTE) {
        store.Add(doc_id, doc);
        // check document body
        REQUIRE(store.Get(doc_id) == doc);
    }
}


TEST_CASE("String to char *, back to string works", "[String_To_Char*]") {
    std::string test_str= "hello world";
    char * buffer;
    int ret = posix_memalign((void **)&buffer, PAGE_SIZE, PAGE_SIZE);
    
    std::copy(test_str.begin(), test_str.end(), buffer);
    buffer[test_str.size()] = '\0';

    std::string str(buffer);
    REQUIRE(test_str.compare(str) == 0 );
}

TEST_CASE( "Vector-based posting list works fine", "[posting_list]" ) {
  SECTION("New postings can be added and retrieved") {
    PostingList_Vec<PostingSimple> pl("hello");   

    REQUIRE(pl.Size() == 0);
    pl.AddPosting(PostingSimple(10, 88, Positions{28}));
    REQUIRE(pl.Size() == 1);
    REQUIRE(pl.GetPosting(0).GetDocId() == 10);
    REQUIRE(pl.GetPosting(0).GetTermFreq() == 88);
    REQUIRE(pl.GetPosting(0).GetPositions() == Positions{28});

    pl.AddPosting(PostingSimple(11, 889, Positions{28, 230}));
    REQUIRE(pl.Size() == 2);
    REQUIRE(pl.GetPosting(1).GetDocId() == 11);
    REQUIRE(pl.GetPosting(1).GetTermFreq() == 889);
    REQUIRE(pl.GetPosting(1).GetPositions() == Positions{28, 230});
  }


  SECTION("Skipping works") {
    PostingList_Vec<PostingSimple> pl("hello");   
    for (int i = 0; i < 100; i++) {
      pl.AddPosting(PostingSimple(i, 1, Positions{28}));
    }
    REQUIRE(pl.Size() == 100);

    SECTION("It can stay at starting it") {
      PostingList_Vec<PostingSimple>::iterator_t it;
      it = pl.SkipForward(0, 0);
      REQUIRE(it == 0);

      it = pl.SkipForward(8, 8);
      REQUIRE(it == 8);
    }

    SECTION("It can skip multiple postings") {
      PostingList_Vec<PostingSimple>::iterator_t it;
      it = pl.SkipForward(0, 8);
      REQUIRE(it == 8);

      it = pl.SkipForward(0, 99);
      REQUIRE(it == 99);
    }

    SECTION("It returns pl.Size() when we cannot find doc id") {
      PostingList_Vec<PostingSimple>::iterator_t it;
      it = pl.SkipForward(0, 1000);
      REQUIRE(it == pl.Size());
    }
  }

}


TEST_CASE( "Intersection", "[intersect]" ) {
  PostingList_Vec<PostingSimple> pl01("hello");   
  for (int i = 0; i < 10; i++) {
    pl01.AddPosting(PostingSimple(i, 1, Positions{28}));
  }

  SECTION("It intersects a subset") {
    PostingList_Vec<PostingSimple> pl02("world");   
    for (int i = 5; i < 10; i++) {
      pl02.AddPosting(PostingSimple(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{5, 6, 7, 8, 9});
  }

  SECTION("It intersects an empty posting list") {
    PostingList_Vec<PostingSimple> pl02("world");   

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{});
  }

  SECTION("It intersects a non-overlapping posting list") {
    PostingList_Vec<PostingSimple> pl02("world");   
    for (int i = 10; i < 20; i++) {
      pl02.AddPosting(PostingSimple(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{});
  }

  SECTION("It intersects a partial-overlapping posting list") {
    PostingList_Vec<PostingSimple> pl02("world");   
    for (int i = 5; i < 15; i++) {
      pl02.AddPosting(PostingSimple(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{5, 6, 7, 8, 9});
  }

  SECTION("It intersects a super set") {
    PostingList_Vec<PostingSimple> pl02("world");   
    for (int i = 0; i < 15; i++) {
      pl02.AddPosting(PostingSimple(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  }

  SECTION("It intersects a single list") {
    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  }

  SECTION("It intersects three lists") {
    PostingList_Vec<PostingSimple> pl02("world");   
    for (int i = 0; i < 5; i++) {
      pl02.AddPosting(PostingSimple(i, 1, Positions{28}));
    }

    PostingList_Vec<PostingSimple> pl03("more");   
    for (int i = 0; i < 2; i++) {
      pl03.AddPosting(PostingSimple(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02, &pl03};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{0, 1});
  }

  SECTION("It intersects two empty list") {
    PostingList_Vec<PostingSimple> pl01("hello");   
    PostingList_Vec<PostingSimple> pl02("world");   

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{});
  }

  SECTION("It handles a previous bug") {
    PostingList_Vec<PostingSimple> pl01("hello");   
    pl01.AddPosting(PostingSimple(8, 1, Positions{28}));
    pl01.AddPosting(PostingSimple(10, 1, Positions{28}));

    PostingList_Vec<PostingSimple> pl02("world");   
    pl02.AddPosting(PostingSimple(7, 1, Positions{28}));
    pl02.AddPosting(PostingSimple(10, 1, Positions{28}));
    pl02.AddPosting(PostingSimple(15, 1, Positions{28}));

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    std::vector<DocIdType> ret = intersect<PostingSimple>(lists);
    REQUIRE(ret == std::vector<DocIdType>{10});
  }


  SECTION("It returns doc counts of each term") {
    PostingList_Vec<PostingSimple> pl02("world");   
    for (int i = 0; i < 15; i++) {
      pl02.AddPosting(PostingSimple(i, 1, Positions{28}));
    }

    std::vector<const PostingList_Vec<PostingSimple>*> lists{&pl01, &pl02};
    IntersectionResult result;

    std::vector<DocIdType> ret = intersect<PostingSimple>(lists, &result);
    REQUIRE(ret == std::vector<DocIdType>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    REQUIRE(result.GetDocCount("hello") == 10);
    REQUIRE(result.GetDocCount("world") == 15);
  }

  SECTION("It returns term freqs") {
    PostingList_Vec<StandardPosting> pl01("hello");   
    pl01.AddPosting(StandardPosting(8, 1));
    pl01.AddPosting(StandardPosting(10, 2));

    PostingList_Vec<StandardPosting> pl02("world");   
    pl02.AddPosting(StandardPosting(7, 1));
    pl02.AddPosting(StandardPosting(10, 8));
    pl02.AddPosting(StandardPosting(15, 1));

    std::vector<const PostingList_Vec<StandardPosting>*> lists{&pl01, &pl02};
    IntersectionResult result;

    std::vector<DocIdType> ret = intersect<StandardPosting>(lists, &result);

    REQUIRE(ret == std::vector<DocIdType>{10});
    REQUIRE(result.GetPosting(10, "hello")->GetTermFreq() == 2);
    REQUIRE(result.GetPosting(10, "world")->GetTermFreq() == 8);
  }

  SECTION("It returns term freqs (new)") {
    PostingList_Vec<StandardPosting> pl01("hello");   
    pl01.AddPosting(StandardPosting(8, 1));
    pl01.AddPosting(StandardPosting(10, 2));

    PostingList_Vec<StandardPosting> pl02("world");   
    pl02.AddPosting(StandardPosting(7, 1));
    pl02.AddPosting(StandardPosting(10, 8));
    pl02.AddPosting(StandardPosting(15, 1));

    std::vector<const PostingList_Vec<StandardPosting>*> lists{&pl01, &pl02};
    IntersectionResult result;

    std::vector<DocIdType> ret = intersect<StandardPosting>(lists, &result);

    REQUIRE(ret == std::vector<DocIdType>{10});
    REQUIRE(result.GetPosting(10, "hello")->GetTermFreq() == 2);
    REQUIRE(result.GetPosting(10, "world")->GetTermFreq() == 8);
  }

}

TEST_CASE( "DocLengthStore", "[ranking]" ) {
  SECTION("It can add some lengths.") {
    DocLengthStore store;
    store.AddLength(1, 7);
    store.AddLength(2, 8);
    store.AddLength(3, 9);

    REQUIRE(store.Size() == 3);
    REQUIRE(store.GetAvgLength() == 8);
  }
}


TEST_CASE( "Scoring", "[ranking]" ) {
  SECTION("TF is correct") {
    REQUIRE(calc_tf(1) == 1.0); // sample in ES document
    REQUIRE(calc_tf(4) == 2.0);
    REQUIRE(calc_tf(100) == 10.0);

    REQUIRE(utils::format_double(calc_tf(2), 3) == "1.41");
  }

  SECTION("IDF is correct (sample score in ES document)") {
    REQUIRE(utils::format_double(calc_idf(1, 1), 3) == "0.307");
  }


  SECTION("Field length norm is correct") {
    REQUIRE(calc_field_len_norm(4) == 0.5);
  }

  SECTION("ElasticSearch IDF") {
    REQUIRE(utils::format_double(calc_es_idf(1, 1), 3) == "0.288"); // From an ES run
  }

  SECTION("ElasticSearch IDF 2") {
    REQUIRE(utils::format_double(calc_es_idf(3, 1), 3)== "0.981"); // From an ES run
  }

  SECTION("ElasticSearch TF NORM") {
    REQUIRE(calc_es_tfnorm(1, 3, 3.0) == 1.0); // From an ES run
    REQUIRE(calc_es_tfnorm(1, 7, 7.0) == 1.0); // From an ES run
    REQUIRE( utils::format_double(calc_es_tfnorm(1, 2, 8/3.0), 3) == "1.11"); // From an ES run
  }
}



TEST_CASE( "We can get score for each document", "[ranking]" ) {
  SECTION("It gets the same score as ES, for one term") {
    IntersectionResult result;
    StandardPosting posting(0, 1); // id=0, term_freq=1

    result.SetPosting(0, "term1", &posting);
    result.SetDocCount("term1", 1);

    IntersectionResult::row_iterator it = result.row_cbegin();

    TermScoreMap term_scores = score_terms_in_doc(result, it, 3, 3, 1);

    REQUIRE(utils::format_double(term_scores["term1"], 3) == "0.288"); // From an ES run
  }

  SECTION("It gets the same score as ES, for two terms") {
    StandardPosting p0(0, 1), p1(0, 1);
    IntersectionResult result;

    result.SetPosting(0, "term1", &p0);
    result.SetPosting(0, "term2", &p1);

    result.SetDocCount("term1", 1);
    result.SetDocCount("term2", 1);

    IntersectionResult::row_iterator it = result.row_cbegin();

    TermScoreMap term_scores = score_terms_in_doc(result, it, 7, 7, 1);

    REQUIRE(utils::format_double(term_scores["term1"], 3) == "0.288"); // From an ES run
    REQUIRE(utils::format_double(term_scores["term2"], 3) == "0.288"); // From an ES run
  }

}

TEST_CASE( "Utilities work", "[utils]" ) {
  SECTION("Count terms") {
    REQUIRE(utils::count_terms("hello world") == 2);
  }

  SECTION("Count tokens") {
    utils::CountMapType counts = utils::count_tokens("hello hello you");
    assert(counts["hello"] == 2);
    assert(counts["you"] == 1);
    assert(counts.size() == 2);
  }
}


void setup_inverted_index(InvertedIndexService &inverted_index) {
  inverted_index.AddDocument(0, DocInfo("hello world", "hello world"));
  inverted_index.AddDocument(1, DocInfo("hello wisconsin", "hello wisconsin"));
  REQUIRE(inverted_index.Size() == 3);
}

void test_inverted_index(InvertedIndexService &inverted_index) {
    auto iterators = inverted_index.FindIterators(TermList{"hello"});
    auto it = iterators[0].get();
    {
      REQUIRE(it->DocId() == 0);
      auto pairs_it = it->OffsetPairsBegin();
      OffsetPair pair;
      pairs_it->Pop(&pair);
      REQUIRE(pair == std::make_tuple(0, 4)); // in doc 0
    }

    auto ret = it->Advance();
    REQUIRE(ret == true);
    {
      REQUIRE(it->DocId() == 1);
      auto pairs_it = it->OffsetPairsBegin();
      OffsetPair pair;
      pairs_it->Pop(&pair);
      REQUIRE(pair == std::make_tuple(0, 4)); // in doc 0
    }
}

TEST_CASE( "Inverted index", "[engine]" ) {
  // SECTION("Unompressed") {
    // InvertedIndexQqMemVec inverted_index;

    // setup_inverted_index(inverted_index);
    // test_inverted_index(inverted_index);
  // }

  SECTION("Compressed") {
    InvertedIndexQqMemDelta inverted_index;

    setup_inverted_index(inverted_index);
    test_inverted_index(inverted_index);
  }
}

// compressed or uncompressed
QqMemEngine test_get_engine(std::string inverted_index) {
  GeneralConfig config;
  config.SetString("inverted_index", inverted_index);
  QqMemEngine engine(config);

  engine.AddDocument(DocInfo("hello world", "hello world"));
  REQUIRE(engine.TermCount() == 2);

  engine.AddDocument(DocInfo("hello wisconsin", "hello wisconsin"));
  REQUIRE(engine.TermCount() == 3);

  engine.AddDocument(DocInfo("hello world big world", "hello world big world"));
  REQUIRE(engine.TermCount() == 4);

  return engine;
}

void test_01(SearchEngineServiceNew &engine) {
    SearchResult result = engine.Search(SearchQuery(TermList{"wisconsin"}));

    REQUIRE(result.Size() == 1);
    REQUIRE(result.entries[0].doc_id == 1);
    REQUIRE(utils::format_double(result.entries[0].doc_score, 3) == "1.09");
}

void test_02(SearchEngineServiceNew &engine) {
    SearchResult result = engine.Search(SearchQuery(TermList{"hello"}));
    REQUIRE(result.Size() == 3);

    // The score below is produced by ../tools/es_index_docs.py in this
    // same git commit
    // You can reproduce the elasticsearch score by checking out this
    // commit and run `python tools/es_index_docs.py`.
    REQUIRE(utils::format_double(result.entries[0].doc_score, 3) == "0.149");
    REQUIRE(utils::format_double(result.entries[1].doc_score, 3) == "0.149");
    REQUIRE(utils::format_double(result.entries[2].doc_score, 3) == "0.111");
}

void test_03(SearchEngineServiceNew &engine) {
    SearchResult result = engine.Search(SearchQuery(TermList{"hello", "world"}));

    REQUIRE(result.Size() == 2);
    // The scores below are produced by ../tools/es_index_docs.py in this
    // same git commit
    // You can reproduce the elasticsearch scores by checking out this
    // commit and run `python tools/es_index_docs.py`.
    REQUIRE(utils::format_double(result.entries[0].doc_score, 3) == "0.677");
    REQUIRE(utils::format_double(result.entries[1].doc_score, 3) == "0.672");
}

void test_04(SearchEngineServiceNew &engine) {
    auto result = engine.Search(SearchQuery(TermList{"hello"}, true));
    REQUIRE(result.Size() == 3);
    // We cannot test the snippets of the first entries because we do not know
    // their order (the first two entries have the same score).
    REQUIRE(result.entries[2].snippet == "<b>hello<\\b> world big world\n");
}

void test_05(SearchEngineServiceNew &engine) {
    auto result = engine.Search(SearchQuery(TermList{"hello", "world"}, true));
    REQUIRE(result.Size() == 2);

    REQUIRE(result.entries[0].snippet == "<b>hello<\\b> <b>world<\\b> big <b>world<\\b>\n");
    REQUIRE(result.entries[1].snippet == "<b>hello<\\b> <b>world<\\b>\n");
}

void test_06(SearchEngineServiceNew &engine) {
    auto query = SearchQuery(TermList{"hello", "world"}, true);
    query.n_results = 0;
    auto result = engine.Search(query);
    REQUIRE(result.Size() == 0);
}

TEST_CASE( "QQ Mem Compressed Engine works", "[engine]" ) {
  auto engine = test_get_engine("compressed");

  SECTION("The engine can serve single-term queries") {
    test_01(engine);
  }

  SECTION("The engine can serve single-term queries with multiple results") {
    test_02(engine);
  }

  SECTION("The engine can server two-term queries") {
    test_03(engine);
  }

  SECTION("It can generate snippets") {
    test_04(engine);
  }

  SECTION("It can generate snippets for two-term query") {
    test_05(engine);
  }

  SECTION("The engine behaves correct when n_results is 0") {
    test_06(engine);
  }
}

TEST_CASE( "QQ Mem Uncompressed Engine works", "[engine]" ) {
  auto engine = test_get_engine("uncompressed");

  SECTION("The engine can serve single-term queries") {
    test_01(engine);
  }

  SECTION("The engine can serve single-term queries with multiple results") {
    test_02(engine);
  }

  SECTION("The engine can server two-term queries") {
    test_03(engine);
  }

  SECTION("It can generate snippets") {
    test_04(engine);
  }

  SECTION("It can generate snippets for two-term query") {
    test_05(engine);
  }

  SECTION("The engine behaves correct when n_results is 0") {
    test_06(engine);
  }
}






TEST_CASE( "Sorting document works", "[ranking]" ) {
  SECTION("A regular case") {
    DocScoreVec scores{{0, 0.8}, {1, 3.0}, {2, 2.1}};    
    REQUIRE(utils::find_top_k(scores, 4) == std::vector<DocIdType>{1, 2, 0});
    REQUIRE(utils::find_top_k(scores, 3) == std::vector<DocIdType>{1, 2, 0});
    REQUIRE(utils::find_top_k(scores, 2) == std::vector<DocIdType>{1, 2});
    REQUIRE(utils::find_top_k(scores, 1) == std::vector<DocIdType>{1});
    REQUIRE(utils::find_top_k(scores, 0) == std::vector<DocIdType>{});
  }

  SECTION("Empty scores") {
    DocScoreVec scores{};    
    REQUIRE(utils::find_top_k(scores, 4) == std::vector<DocIdType>{});
  }

  SECTION("Identical scores") {
    DocScoreVec scores{{0, 0.8}, {1, 0.8}, {2, 2.1}};    
    REQUIRE(utils::find_top_k(scores, 1) == std::vector<DocIdType>{2});
  }
}


TEST_CASE( "Class Staircase can generate staircase strings", "[utils]" ) {
  SECTION("Simple case") {
    utils::Staircase staircase(1, 1, 2);
    
    REQUIRE(staircase.MaxWidth() == 2);

    REQUIRE(staircase.NextLayer() == "0");
    REQUIRE(staircase.NextLayer() == "0 1");
    REQUIRE(staircase.NextLayer() == "");
  }

  SECTION("Simple case 2") {
    utils::Staircase staircase(2, 1, 2);

    REQUIRE(staircase.MaxWidth() == 2);

    REQUIRE(staircase.NextLayer() == "0");
    REQUIRE(staircase.NextLayer() == "0");
    REQUIRE(staircase.NextLayer() == "0 1");
    REQUIRE(staircase.NextLayer() == "0 1");
    REQUIRE(staircase.NextLayer() == "");
  }

  SECTION("Wide steps") {
    utils::Staircase staircase(2, 3, 2);

    REQUIRE(staircase.MaxWidth() == 6);

    REQUIRE(staircase.NextLayer() == "0 1 2");
    REQUIRE(staircase.NextLayer() == "0 1 2");
    REQUIRE(staircase.NextLayer() == "0 1 2 3 4 5");
    REQUIRE(staircase.NextLayer() == "0 1 2 3 4 5");
    REQUIRE(staircase.NextLayer() == "");
  }
}


