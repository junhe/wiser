#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "qq_engine.h"
#include "lrucache.h"
#include <boost/locale.hpp>
#include <math.h> 
#include <fstream>

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
        std::string to_string(std::string * doc_string);
};


class SentenceBreakIterator {

    public:
        SentenceBreakIterator(std::string & content);

        int getStartOffset();  // get current sentence's start offset
        int getEndOffset();    // get current sentence's end offset
        int next();            // get next sentence
        int next(int offset);  // get next sentence where offset is within it
        std::string * content_;
    
    private:
        boost::locale::generator gen;
        boost::locale::boundary::sboundary_point_index map;    //TODO extra copy operation
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
        std::vector<std::string> highlight(const Query & query, const TopDocs & topDocs, const int & maxPassages);

        std::string highlightForDoc(const Query & query, const int & docID, const int & maxPassages);
        // get each query term's offsets iterator
        OffsetsEnums getOffsetsEnums(const Query & query, const int & docID);
        // highlight using the iterators and the content of the document
        std::string highlightOffsetsEnums(const OffsetsEnums & offsetsEnums, const int & docID, const int & maxPassages);
       
        // helper for generating key in hash table 
        std::string construct_key(const Query & query, const int & docID);
    
    private:
        // cache for snippets ( docID+query -> string )
        cache::lru_cache<std::string, std::string> _snippets_cache_ {cache::lru_cache<std::string, std::string>(SNIPPETS_CACHE_SIZE)};
        cache::lru_cache<std::string, int> _snippets_cache_falsh_ {cache::lru_cache<std::string, int>(SNIPPETS_CACHE_ON_FLASH_SIZE)};
        //TODO file to hold the cache


        // passage normalization function for scoring
        float pivot = 87;  // hard-coded average length of passage
        float k1 = 1.2;  // BM25 parameters
        float b = 0.75;  // BM25 parameters
        float passage_norm(int & start_offset);
        float tf_norm(int freq, int passageLen);
}; 


#endif
