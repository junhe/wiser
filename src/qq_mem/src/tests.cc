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
    Posting posting(100, 2, Offsets {std::make_tuple(1,3), std::make_tuple(5,8)});
    REQUIRE(posting.docID_ == 100);
    REQUIRE(posting.term_frequency_ == 2);
    // TODO change from postions to offsets
    REQUIRE(posting.positions_[0] == std::make_tuple(1,3));
    REQUIRE(posting.positions_[1] == std::make_tuple(5,8));
}

TEST_CASE( "Direct Posting List essential operations are OK", "[posting_list_direct]" ) {
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
    QQSearchEngine engine;
    
    // read in the linedoc
    utils::LineDoc linedoc("src/testdata/line_doc_offset");
    std::vector<std::string> items;
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
        res += std::to_string(std::get<0>(offset)) + "," + std::to_string(std::get<1>(offset)) + ";";
    }
    res += ".";
    REQUIRE(res == "29,34;94,99;124,129;178,183;196,201;490,495;622,626;1525,1529;1748,1753;2119,2124;2168,2172;2623,2627;4164,4168;4784,4788;6425,6429;7507,7511;9090,9094;9894,9898;11375,11379;12093,12097;12695,12700;.");
}

TEST_CASE( "OffsetsEnums of Unified Highlighter essential operations are OK", "[offsetsenums_unified_highlighter]" ) {
    QQSearchEngine engine;
    
    // read in the linedoc
    utils::LineDoc linedoc("src/testdata/line_doc_offset");
    std::vector<std::string> items;
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
    
    // read in the linedoc
    utils::LineDoc linedoc("src/testdata/line_doc_offset");
    std::vector<std::string> items;
    linedoc.GetRow(items);
    
    
    // adddocument
    engine.AddDocument(items[0], "http://wiki", items[1], items[2], items[3]);
    UnifiedHighlighter test_highlighter(engine);
    

    //start highlighter
    //Query query = {"rule"};
    Query query = {"rule"};
    TopDocs topDocs = {0}; 
    int maxPassages = 10;
   
    std::vector<std::string> res;
    for (int i = 0; i < 3; i++) { 
        struct timeval t1,t2;
        double timeuse;
        gettimeofday(&t1,NULL);
  
        res = test_highlighter.highlight(query, topDocs, maxPassages);
        gettimeofday(&t2,NULL);
        timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
        printf("Use Time:%fs\n",timeuse);
        REQUIRE(res.size() == topDocs.size());
        std::cout << res[0] <<std::endl;
    }

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
