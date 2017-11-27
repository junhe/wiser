#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "inverted_index.h"

typedef TermList Query;
typedef std::vector<int> TopDocs;

class UnifiedHighlighter {

    public: 
        InvertedIndex index_;
         
        UnifiedHighlighter(InvertedIndex index);
        UnifiedHighlighter();
        
        // Return snippets generated for the top-ranked documents
        // query: terms of this query
        // topDocs: array of docID of top-ranked documents
        std::vector<std::string> highlight(Query query, TopDocs topDocs);

    private:
        std::string highlightForDoc(Query query, int docID);
        
}; 


#endif
