#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "qq_engine.h"
#include <tuple>
#include <boost/locale.hpp>

typedef TermList Query;
typedef std::vector<int> TopDocs;
typedef std::tuple<int, int> Offset;
typedef std::vector<std::vector<Offset>::iterator> OffsetsEnums;

class SentenceBreakIterator {

    public:
        SentenceBreakIterator(std::string content);

        int getStartOffset();
        int getEndOffset();
        int next();

    private:
        boost::locale::generator gen;
        boost::locale::boundary::sboundary_point_index map;
        boost::locale::boundary::sboundary_point_index::iterator current; 
        boost::locale::boundary::sboundary_point_index::iterator last; 
        int startoffset;
        int endoffset;
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
        std::string highlightForDoc(Query & query, const int & docID);
        // get each query term's offsets iterator
        OffsetsEnums getOffsetsEnums(Query & query, const int & docID);
        // highlight using the iterators and the content of the document
        std::string highlightOffsetsEnums(OffsetsEnums & offsetsEnums, const int & docID);
        
}; 


#endif
