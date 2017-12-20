// #define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include <iostream>
#include <set>
#include <sys/time.h>
#include <boost/filesystem.hpp>
#include <boost/lambda/lambda.hpp>    
#include "catch.hpp"

#include "native_doc_store.h"
#include "inverted_index.h"
#include "qq_engine.h"
#include "index_creator.h"
#include "utils.h"

#include "posting_list_direct.h"
#include "posting_list_raw.h"
#include "posting_list_protobuf.h"

#include "unifiedhighlighter.h"

#include "lrucache.h"

// cereal
#include <cereal/archives/binary.hpp>
#include <sstream>


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
    if (FLAG_DOCUMENTS_ON_FLASH) 
        //TODO: support has etc.? 
        return;
 
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
    if (FLAG_SNIPPETS_PRECOMPUTE || FLAG_POSTINGS_ON_FLASH)
        return ;
    InvertedIndex index;     
    index.AddDocument(100, TermList{"hello", "world"});
    index.AddDocument(101, TermList{"hello", "earth"});

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

TEST_CASE( "Basic Posting", "[posting]" ) {
    if (FLAG_SNIPPETS_PRECOMPUTE || FLAG_POSTINGS_ON_FLASH)
        return;
    Posting posting(100, 2, Offsets {std::make_tuple(1,3), std::make_tuple(5,8)});
    REQUIRE(posting.docID_ == 100);
    REQUIRE(posting.term_frequency_ == 2);
    // TODO change from postions to offsets
    REQUIRE(posting.positions_[0] == std::make_tuple(1,3));
    REQUIRE(posting.positions_[1] == std::make_tuple(5,8));
}

TEST_CASE( "Direct Posting List essential operations are OK", "[posting_list_direct]" ) {
    if (FLAG_SNIPPETS_PRECOMPUTE || FLAG_POSTINGS_ON_FLASH)
        return;
    PostingList_Direct pl("term001");
    REQUIRE(pl.Size() == 0);

    pl.AddPosting(111, 1,  Offsets {std::make_tuple(19,91)});
    pl.AddPosting(232, 2,  Offsets {std::make_tuple(10,18), std::make_tuple(19,91)});

    REQUIRE(pl.Size() == 2);

    for (auto it = pl.cbegin(); it != pl.cend(); it++) {
        auto p = pl.ExtractPosting(it); 
        REQUIRE((p.docID_ == 111 || p.docID_ == 232));
        if (p.docID_ == 111) {
            REQUIRE(p.term_frequency_ == 1);
            REQUIRE(p.positions_.size() == 1);
            REQUIRE(p.positions_[0] == std::make_tuple(19,91));
        } else {
            // p.docID_ == 232
            REQUIRE(p.term_frequency_ == 2);
            REQUIRE(p.positions_.size() == 2);
            REQUIRE(p.positions_[0] == std::make_tuple(10,18));
            REQUIRE(p.positions_[1] == std::make_tuple(19,91));
        }
    }
}

TEST_CASE( "Raw String based Posting List essential operations are OK", "[posting_list_raw]" ) {
    /*
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
    // TODO change from positions to offsets
    //REQUIRE(p1.positions_.size() == 1);
    
    Posting p2 = pl1.GetPosting();
    REQUIRE(p2.docID_ == 232);
    REQUIRE(p2.term_frequency_ == 2);
    // TODO change from positions to offsets
    //REQUIRE(p2.positions_.size() == 2);
    */
}


TEST_CASE( "Protobuf based Posting List essential operations are OK", "[posting_list_protobuf]" ) {
    /*
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
    // TODO change from positions to offsets
    //REQUIRE(p1.positions_.size() == 1);
    
    Posting p2 = pl1.GetPosting();
    REQUIRE(p2.docID_ == 232);
    REQUIRE(p2.term_frequency_ == 2);
    // TODO change from positions to offsets
    //REQUIRE(p2.positions_.size() == 2);
    */
}

TEST_CASE( "QQSearchEngine", "[engine]" ) {
    if (FLAG_SNIPPETS_PRECOMPUTE || FLAG_POSTINGS_ON_FLASH)
        return;

    QQSearchEngine engine;

    REQUIRE(engine.NextDocId() == 0);

    engine.AddDocument("my title", "my url", "my body");

    std::vector<int> doc_ids = engine.Search(TermList{"my"}, SearchOperator::AND);
    REQUIRE(doc_ids.size() == 1);

    std::string doc = engine.GetDocument(doc_ids[0]);
    REQUIRE(doc == "my body");
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

TEST_CASE( "Search Engine with offsets essential operations are OK", "[search_engine_offsets]" ) {
    if (FLAG_SNIPPETS_PRECOMPUTE || FLAG_POSTINGS_ON_FLASH)
        return;
    
    QQSearchEngine engine;
    
    // read in the linedoc
    utils::LineDoc linedoc("src/testdata/line_doc_offset");
    std::vector<std::string> items;
    linedoc.GetRow(items);
    linedoc.GetRow(items);
    
    // adddocument
    engine.AddDocument(items[0], "http://wiki", items[1], items[2], items[3]);

    // search for "rule"
    std::vector<int> doc_ids = engine.Search(TermList{"rule"}, SearchOperator::AND);
    REQUIRE(doc_ids.size() == 1);

    // check doc
    std::string doc = engine.GetDocument(doc_ids[0]);
    REQUIRE(doc == items[1]);

    // check posting
    Posting result = engine.inverted_index_.GetPosting("rule", doc_ids[0]);
    REQUIRE(result.docID_ == doc_ids[0]);
    REQUIRE(result.term_frequency_ == 21);
    REQUIRE(result.positions_.size() == 21);
    // check Offsets
    std::string res = "";
    for (auto offset:result.positions_) {
        int startoffset = std::get<0>(offset);
        int endoffset = std::get<1>(offset);

        res += std::to_string(startoffset) + "," + std::to_string(endoffset) + ";";
        // check string
        //std::cout << items[1].substr(startoffset, endoffset-startoffset+1) << std::endl;
    }
    res += ".";
    REQUIRE(res == "29,34;94,99;124,129;178,183;196,201;490,495;622,626;1525,1529;1748,1753;2119,2124;2168,2172;2623,2627;4164,4168;4784,4788;6425,6429;7507,7511;9090,9094;9894,9898;11375,11379;12093,12097;12695,12700;.");


    
}

TEST_CASE( "OffsetsEnums of Unified Highlighter essential operations are OK", "[offsetsenums_unified_highlighter]" ) {
    if (FLAG_SNIPPETS_PRECOMPUTE || FLAG_POSTINGS_ON_FLASH)
        return;

    QQSearchEngine engine;
    
    // read in the linedoc
    utils::LineDoc linedoc("src/testdata/line_doc_offset");
    std::vector<std::string> items;
    linedoc.GetRow(items);
    linedoc.GetRow(items);
    
    
    // adddocument
    engine.AddDocument(items[0], "http://wiki", items[1], items[2], items[3]);
    UnifiedHighlighter test_highlighter(engine);
    Query query = {"rule"};
    
    OffsetsEnums offsetsEnums = test_highlighter.getOffsetsEnums(query, 0);
    REQUIRE(offsetsEnums.size() == 1);

    // check iterator
    Offset_Iterator cur_iter = offsetsEnums[0];
    std::string res = "";
    while (1) {
        // judge whether end
        if (cur_iter.startoffset == -1) {
            break;
        }
        // print current position
        res += std::to_string(cur_iter.startoffset) +  "," + std::to_string(cur_iter.endoffset) + ";";
        cur_iter.next_position();
    } 
    res += ".";
    REQUIRE(res == "29,34;94,99;124,129;178,183;196,201;490,495;622,626;1525,1529;1748,1753;2119,2124;2168,2172;2623,2627;4164,4168;4784,4788;6425,6429;7507,7511;9090,9094;9894,9898;11375,11379;12093,12097;12695,12700;.");
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

TEST_CASE( "Unified Highlighter essential operations are OK", "[unified_highlighter]" ) {
    QQSearchEngine engine;
    UnifiedHighlighter test_highlighter(engine);
    
    // read in the linedoc
    utils::LineDoc linedoc("src/testdata/line_doc_offset");
    std::vector<std::string> items;
    linedoc.GetRow(items);   // 884B
    linedoc.GetRow(items);   // 15KB
    linedoc.GetRow(items);   // 177KB
    linedoc.GetRow(items);   // 1MB
    linedoc.GetRow(items); // 8KB
    
    // adddocument
    engine.AddDocument(items[0], "http://wiki", items[1], items[2], items[3]);

    //start highlighter
    //Query query = {"park"}; // attack build knife zoo
    //Query query = {"rule"}; // we doctor incorrect problem
    //Query query = {"author"}; // similar life accord code
    //Query query = {"mondai"}; // support student report telephon
    Query query = {"polic"};  // bulletin inform law system
    
    // terms
    //Query query = {"park", "attack", "build", "knife", "zoo"};
    //Query query = {"rule", "we", "doctor", "incorrect", "problem"};
    //Query query = {"author", "similar", "life", "accord", "code"};
    //Query query = {"mondai", "support", "student", "report", "telephon"};
    //Query query = {"polic", "bulletin", "inform", "law", "system"};  // bulletin
    TopDocs topDocs = {0}; 
    

    int maxPassages = 5;
    //int maxPassages = 10;
    //int maxPassages = 20;
    //int maxPassages = 30;
    //int maxPassages = 40;
    //int maxPassages = 50;
   
    // warm up
    //test_highlighter.highlight({"support"}, topDocs, maxPassages);

    struct timeval t1,t2;
    double timeuse;
    std::vector<std::string> res;
    for (int i = 0; i < 4; i++) { 
        // clear the cache
        engine.inverted_index_.clear_posting_cache();
        engine.doc_store_.clear_caches();

        flag_posting = false;
        gettimeofday(&t1,NULL);
        //TopDocs topDocs = engine.Search(query, SearchOperator::AND);
        res = test_highlighter.highlight(query, topDocs, maxPassages);
        gettimeofday(&t2,NULL);
        timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
        printf("Use Time:%fms\n",timeuse*1000);
        REQUIRE(res.size() == topDocs.size());
        std::cout << "Get there!" << std::endl;
        std::cout << res[0] <<std::endl;
    }
    
}
// TODO test case for repcompute score:


TEST_CASE( "ScoreEnums of Pre-computation based Unified Highlighter essential operations are OK", "[scoresenums_precompute_unified_highlighter]" ) {
    if (!FLAG_SNIPPETS_PRECOMPUTE)
        return;
    if (FLAG_POSTINGS_ON_FLASH)
        return;   //TODO
    QQSearchEngine engine;
    
    // read in the linedoc
    utils::LineDoc linedoc("src/testdata/line_doc_offset");
    std::vector<std::string> items;
    linedoc.GetRow(items);
    linedoc.GetRow(items);
    
    
    // adddocument
    engine.AddDocument(items[0], "http://wiki", items[1], items[2], items[3]);
    UnifiedHighlighter test_highlighter(engine);
    Query query = {"problem"};
   
    ScoresEnums scoresEnums = test_highlighter.get_passages_scoresEnums(query, 0);
    REQUIRE(scoresEnums.size() == 1);

    // check iterator
    PassageScore_Iterator cur_iter = scoresEnums[0];
    std::string res = "";
    while (1) {
        // judge whether end
        if (cur_iter.cur_passage_id_ == -1) {
            break;
        }
        // print current position
        res += std::to_string(cur_iter.cur_passage_id_) +  "," + std::to_string(cur_iter.score_) + ";";
        cur_iter.next_passage();
    } 
    res += ".";
    REQUIRE(res == "38,0.428823;39,0.609594;113,0.351342;.");
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

TEST_CASE( "UnifiedHighlighter snippets cache key constructor  essential opeartions are OK", "[UnifiedHighlighter_ConstructKey]") {
    // create cache
    QQSearchEngine engine;
    UnifiedHighlighter test_highlighter(engine);

    // construct key
    std::string res = test_highlighter.construct_key({"hello", "world"}, 3);
    REQUIRE(res == "3:hello,world,");
}

TEST_CASE("Precompute and store document sentence segments successfully", "[Precompute_StorePassages]") {
    NativeDocStore store;
    int doc_id = 88;
    std::string doc = "it is a doc. We are the second sentence. I'm the third sentence! ";
    store.Add(doc_id, doc);
    
    if (FLAG_SNIPPETS_PRECOMPUTE) {
        store.Add(doc_id, doc);
        // check document body
        REQUIRE(store.Get(doc_id) == doc);
        // check document passages
        REQUIRE(store.GetPassages(doc_id).size() == 3 );
        REQUIRE(store.GetPassages(doc_id)[0] == Passage_Segment(0,13) );
        REQUIRE(store.GetPassages(doc_id)[1] == Passage_Segment(13,28) );
        REQUIRE(store.GetPassages(doc_id)[2] == Passage_Segment(41,24) );
    }
}

// TODO: test cases for precomputed based snippet generating

TEST_CASE("Serialization tools essential operations work", "[Serialization_Tools]") {
    // protobuf, using posting_basic
  
    if (!FLAG_SNIPPETS_PRECOMPUTE)
        return;
    Offsets positions = {std::make_tuple(1,3), std::make_tuple(4,5), std::make_tuple(6,8), std::make_tuple(-1,-1),
                         std::make_tuple(1,1234567), std::make_tuple(0,2), std::make_tuple(3,8323232), std::make_tuple(2,1)};
    Posting new_posting(5, 0, positions);
/*    std::cout << new_posting.docID_ << std::endl;
    std::cout << new_posting.term_frequency_ << std::endl;
    std::cout << new_posting.passage_scores_.size() << ": " << new_posting.passage_scores_[0].first << "," <<  new_posting.passage_scores_[0].second << ";" << new_posting.passage_scores_[1].first <<","<< new_posting.passage_scores_[1].second << ";"<< std::endl;
    std::cout << new_posting.passage_splits_.size() << ": " << new_posting.passage_splits_[1].first <<"," << new_posting.passage_splits_[1].second << ";" << new_posting.passage_splits_[3].first << "," << new_posting.passage_splits_[3].second << ";" << std::endl;
*/
    // serialize using protobuf 
    Store_Segment store_position = Global_Posting_Store->append(new_posting.dump());
    
    std::string serialized = Global_Posting_Store->read(store_position);

    // deserialize
    posting_message::Posting_Precomputed_4_Snippets p_message;
    p_message.ParseFromString(serialized);

 /*   std::cout << p_message.docid() << std::endl;
    std::cout << p_message.term_frequency() << std::endl;
    std::cout << p_message.offsets_size() << ": "
              << p_message.offsets(0).start_offset() << "," << p_message.offsets(0).end_offset() << ";"
              << p_message.offsets(1).start_offset() << "," << p_message.offsets(1).end_offset() << ";"
              << p_message.offsets(2).start_offset() << "," << p_message.offsets(2).end_offset() << ";"
              << std::endl;
    std::cout << p_message.passage_scores_size() << ": " 
              << p_message.passage_scores(0).passage_id() << "," << p_message.passage_scores(0).score() << ";"
              << p_message.passage_scores(1).passage_id() << "," << p_message.passage_scores(1).score() << ";"
              << std::endl;
    std::cout << p_message.passage_splits_size() << ": " 
              << p_message.passage_splits().at(1).start_offset() << "," << p_message.passage_splits().at(1).len() << ";"
              << p_message.passage_splits().at(3).start_offset() << "," << p_message.passage_splits().at(3).len() << ";"
              << std::endl;
  */ 
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



TEST_CASE("Cereal serialization works", "[Cereal_Serialization]") {
    if (!FLAG_SNIPPETS_PRECOMPUTE)
        return;
    Offsets positions = {std::make_tuple(1,3), std::make_tuple(4,5), std::make_tuple(6,8), std::make_tuple(-1,-1),
                         std::make_tuple(1,1234567), std::make_tuple(0,2), std::make_tuple(3,8323232), std::make_tuple(2,1)};
    Posting new_posting(5, 0, positions);
    
    // serialize using cereal
    /*Store_Segment store_position = Global_Posting_Store->append(new_posting.dump());
    
    std::string serialized = Global_Posting_Store->read(store_position);

    // deserialize
    posting_message::Posting_Precomputed_4_Snippets p_message;
    p_message.ParseFromString(serialized);
    */


    std::stringstream ss; // any stream can be used

   {
    cereal::BinaryOutputArchive oarchive(ss); // Create an output archive

    //int m1, m2, m3;
    //m1 = 1; m2 = 2; m3 = 3;
    oarchive(new_posting); // Write the data to the archive
   } // archive goes out of scope, ensuring all contents are flushed

   {
    cereal::BinaryInputArchive iarchive(ss); // Create an input archive

    Posting tmp_posting;
    iarchive(tmp_posting); // Read the data from the archive
    /*std::cout << tmp_posting.docID_ << std::endl;
    std::cout << tmp_posting.term_frequency_ << std::endl;
    std::cout << tmp_posting.positions_.size() << std::endl;
    std::cout << tmp_posting.positions_.size() << ": " 
              << std::get<0>(tmp_posting.positions_[0]) << "," <<  std::get<1>(tmp_posting.positions_[0]) << ";"
              << std::get<0>(tmp_posting.positions_[1]) << "," <<  std::get<1>(tmp_posting.positions_[1]) << ";"
              << std::get<0>(tmp_posting.positions_[2]) << "," <<  std::get<1>(tmp_posting.positions_[2]) << ";"
              << std::endl;
    std::cout << tmp_posting.passage_scores_.size() << ": " 
              << tmp_posting.passage_scores_[0].first << "," <<  tmp_posting.passage_scores_[0].second << ";"
              << tmp_posting.passage_scores_[1].first << "," <<  tmp_posting.passage_scores_[1].second << ";"
              << std::endl;
    std::cout << tmp_posting.passage_splits_.size() << ": " 
              << tmp_posting.passage_splits_[1].first << "," <<  tmp_posting.passage_splits_[1].second << ";"
              << tmp_posting.passage_splits_[3].first << "," <<  tmp_posting.passage_splits_[3].second << ";"
              << std::endl;
    */
   } 
}
