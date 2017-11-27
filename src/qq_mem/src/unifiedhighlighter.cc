#include "unifiedhighlighter.h"
#include <iostream>
UnifiedHighlighter::UnifiedHighlighter(QQSearchEngine & engine) {
    engine_ = engine;
}

UnifiedHighlighter::UnifiedHighlighter() {
    // for test
}

std::vector<std::string> UnifiedHighlighter::highlight(Query & query, TopDocs & topDocs) {
    std::vector<std::string> res = {}; // size of topDocs
    for(TopDocs::iterator cur_doc = topDocs.begin(); cur_doc != topDocs.end(); ++cur_doc) {
        std::string cur_snippet = highlightForDoc(query, *cur_doc);
        res.push_back( cur_snippet );
    }
    return res;
}



std::string UnifiedHighlighter::highlightForDoc(Query & query, const int & docID) {
    // get offsets: List<OffsetsEnum> offsetsEnums = fieldOffsetStrategy.getOffsetsEnums(reader, docId, content);   TODO???
    
    // highlight: passages = highlightOffsetsEnums(offsetsEnums);   TODO???
    // 
    return engine_.GetDocument(docID);
}
