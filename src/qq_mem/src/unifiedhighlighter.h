#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "qq_engine.h"
#include <tuple>
#include <boost/locale.hpp>

typedef TermList Query;
typedef std::vector<int> TopDocs;
typedef std::tuple<int, int> Offset;

class Offset_Iterator {
    public:
        Offset_Iterator(std::vector<Offset> & offsets_in);
        int startoffset;
        int endoffset;
        void next_position();  // go to next offset position
    private:
        std::vector<Offset> * offsets;
        std::vector<Offset>::iterator cur_position;
        

};

typedef std::vector<Offset_Iterator> OffsetsEnums;

class Passage {
    public:
        int startoffset = -1;
        int endoffset = -1;
        float score = 0;
};


class SentenceBreakIterator {

    public:
        SentenceBreakIterator(std::string content);

        int getStartOffset();  // get current sentence's start offset
        int getEndOffset();    // get current sentence's end offset
        int next();            // get next sentence

    private:
        boost::locale::generator gen;
        boost::locale::boundary::sboundary_point_index map;
        boost::locale::boundary::sboundary_point_index::iterator current; 
        boost::locale::boundary::sboundary_point_index::iterator last; 
        int startoffset;
        int endoffset;
        int last_offset;
        std::string content_;
};

class UnifiedHighlighter {

    public: 
        QQSearchEngine engine_;
         
        UnifiedHighlighter();
        UnifiedHighlighter(QQSearchEngine & engine_);
        
        // highlight:
        // primary method: Return snippets generated for the top-ranked documents
        // query: terms of this query
        // topDocs: array of docID of top-ranked documents
        std::vector<std::string> highlight(Query & query, TopDocs & topDocs, int & maxPassages);

    private:
        // for test
        std::vector<Offset> test_offsets_1 = {std::make_tuple(3, 5), std::make_tuple(9, 10), std::make_tuple(20, 25)};
        std::vector<Offset> test_offsets_2 = {std::make_tuple(6, 7), std::make_tuple(18, 19), std::make_tuple(26, 28)};
        std::vector<Offset> test_offsets_3 = {std::make_tuple(0, 2), std::make_tuple(29, 30), std::make_tuple(32, 35)};

        std::string highlightForDoc(Query & query, const int & docID);
        // get each query term's offsets iterator
        OffsetsEnums getOffsetsEnums(Query & query, const int & docID);
        // highlight using the iterators and the content of the document
        std::string highlightOffsetsEnums(OffsetsEnums & offsetsEnums, const int & docID);
        
}; 


#endif
