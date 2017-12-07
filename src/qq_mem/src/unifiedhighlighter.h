#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "qq_engine.h"
#include <boost/locale.hpp>
#include <math.h> 


class Offset_Iterator {

    public:
        Offset_Iterator(Offsets & offsets_in);
        int startoffset;           // -1 means end
        int endoffset;
        void next_position();      // go to next offset position
        int weight = 1;            // weight of this term

    private:
        Offsets * offsets;
        Offsets::iterator cur_position;
        

};

typedef std::vector<Offset_Iterator> OffsetsEnums;

class Passage {

    public:
        int startoffset = -1;
        int endoffset = -1;
        float score = 0;

        void reset() {
            startoffset = endoffset = -1;
            score = 0;
            matches = {};
        }

        void addMatch(const int & startoffset, const int & endoffset);
        Offsets matches = {};
        std::string to_string(std::string & doc_string);
};


class SentenceBreakIterator {

    public:
        SentenceBreakIterator(std::string content);

        int getStartOffset();  // get current sentence's start offset
        int getEndOffset();    // get current sentence's end offset
        int next();            // get next sentence
        int next(int offset);  // get next sentence where offset is within it
        std::string content_;
    
    private:
        boost::locale::generator gen;
        boost::locale::boundary::sboundary_point_index map;
        boost::locale::boundary::sboundary_point_index::iterator current; 
        boost::locale::boundary::sboundary_point_index::iterator last; 
        int startoffset;
        int endoffset;
        int last_offset;
};

class UnifiedHighlighter {

    public: 
        QQSearchEngine engine_;   // TODO get reference not object
         
        UnifiedHighlighter();
        UnifiedHighlighter(QQSearchEngine & engine_);
        
        // highlight:
        // primary method: Return snippets generated for the top-ranked documents
        // query: terms of this query
        // topDocs: array of docID of top-ranked documents
        std::vector<std::string> highlight(Query & query, TopDocs & topDocs, int & maxPassages);

        std::string highlightForDoc(Query & query, const int & docID, int & maxPassages);
        // get each query term's offsets iterator
        OffsetsEnums getOffsetsEnums(Query & query, const int & docID);
        // highlight using the iterators and the content of the document
        std::string highlightOffsetsEnums(OffsetsEnums & offsetsEnums, const int & docID, int & maxPassages);
    
    private:
        // passage normalization function for scoring
        int pivot = 87;  // hard-coded average length of passage
        float k1 = 1.2;  // BM25 parameters
        float b = 0.75;  // BM25 parameters
        float passage_norm(int & start_offset);
        float tf_norm(int freq, int passageLen);
}; 


#endif
