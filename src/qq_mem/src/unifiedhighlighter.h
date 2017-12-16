#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "qq_engine.h"
#include "lrucache.h"
#include <boost/locale.hpp>
#include <math.h> 
#include <fstream>

class PassageScore_Iterator {

    public:
        PassageScore_Iterator(Passage_Scores & passage_scores_in);
        void next_passage();
        
        int cur_passage_id_;       // -1 means end
        int weight = 1;            // weight of this term
        float score_ = 0;          // score of this passage for this term
    private:
        Passage_Scores * _passage_scores_;
        Passage_Scores::iterator _cur_passage_;

};
typedef std::vector<PassageScore_Iterator> ScoresEnums;

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

        std::string highlightForDoc(const Query & query, const int & docID, const int & maxPassages);  // highlight each document
        OffsetsEnums getOffsetsEnums(const Query & query, const int & docID);  // get each query term's offsets iterator
        std::string highlightOffsetsEnums(const OffsetsEnums & offsetsEnums, const int & docID, const int & maxPassages);  // highlight using the iterators and the content of the document
       
        std::string construct_key(const Query & query, const int & docID); // helper for generating key for search in cache
    
        float passage_norm(const int & start_offset);
        float tf_norm(const int & freq, const int & passageLen);
        
        // highlight based on precomputed scores:
        // primary method highlightQuickForDoc 
        std::string highlightQuickForDoc(const Query & query, const int & docID, const int &maxPassages);  // highlight each document assisted by precomputed scores
        std::vector<int> get_top_passages(const ScoresEnums & scoresEnums, const int & maxPassages); // Helper function for precomputation based snippet generating
        ScoresEnums get_passages_scoresEnums(const Query & query, const int & docID);
        std::string highlight_passages(const Query & query, const int & docID, const std::vector<int> & top_passages); 
    private:
        // cache for snippets ( docID+query -> string )
        cache::lru_cache<std::string, std::string> _snippets_cache_ {cache::lru_cache<std::string, std::string>(SNIPPETS_CACHE_SIZE)};
        // cache for on-flash snippets (docID+query -> position(int) of _snippets_store_)
        cache::lru_flash_cache<std::string, std::string> _snippets_cache_flash_ {cache::lru_flash_cache<std::string, std::string>(SNIPPETS_CACHE_ON_FLASH_SIZE, SNIPPETS_CACHE_ON_FLASH_FILE) };

        // passage normalization parameters for scoring
        float pivot = 87;  // hard-coded average length of passage(according to Lucene)
        float k1 = 1.2;    // BM25 parameters
        float b = 0.75;    // BM25 parameters
        // passage normalization functions for scoring
        //float passage_norm(int & start_offset);
        //float tf_norm(int freq, int passageLen);


}; 


#endif
