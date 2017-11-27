#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "qq_engine.h"

typedef TermList Query;
typedef std::vector<int> TopDocs;

class UnifiedHighlighter {

    public: 
        QQSearchEngine engine_;
         
        UnifiedHighlighter();
        UnifiedHighlighter(QQSearchEngine & engine_);
        
        // highlight:
        // primary method: Return snippets generated for the top-ranked documents
        // query: terms of this query
        // topDocs: array of docID of top-ranked documents
        std::vector<std::string> highlight(Query & query, TopDocs & topDocs);

    private:
        std::string highlightForDoc(Query & query, const int & docID);
        
}; 


#endif
