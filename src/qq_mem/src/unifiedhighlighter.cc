#include "unifiedhighlighter.h"
#include <iostream>
UnifiedHighlighter::UnifiedHighlighter(QQSearchEngine & engine) {
    engine_ = engine;
}

UnifiedHighlighter::UnifiedHighlighter() {
    // for test
}

std::vector<std::string> UnifiedHighlighter::highlight(Query & query, TopDocs & topDocs, int & maxPassages) {
    std::vector<std::string> res = {}; // size of topDocs
    for(TopDocs::iterator cur_doc = topDocs.begin(); cur_doc != topDocs.end(); ++cur_doc) {
        std::string cur_snippet = highlightForDoc(query, *cur_doc);
        res.push_back( cur_snippet );  // TODO currently: maxPassages = 1
    }
    return res;
}



std::string UnifiedHighlighter::highlightForDoc(Query & query, const int & docID) {
    // get offset iterators for each query term for this document
    OffsetsEnums offsetsEnums = getOffsetsEnums(query, docID);

    // highlight according to offset iterators and content of this document
    std::string passage = highlightOffsetsEnums(offsetsEnums, docID);
    
    return passage;
}

OffsetsEnums UnifiedHighlighter::getOffsetsEnums(Query & query, const int & docID) {
    //
    OffsetsEnums res = {};
    for (Query::iterator it = query.begin(); it != query.end(); ++ it) {
        // push back iterator of offsets for this term in this document
    } 
    return res; 
}



std::string UnifiedHighlighter::highlightOffsetsEnums(OffsetsEnums & offsetsEnums, const int & docID) {
    // break the document according to sentence
    BreakIterator breakiterator(engine_.GetDocument(docID));
    // traverse all sentences and print
    // merge sorting

    
    return "Hello world";
}




// BreakIterator
BreakIterator::BreakIterator(std::string content) {
    content_ = content;
    return;
}

