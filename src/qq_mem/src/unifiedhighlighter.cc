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
    while (breakiterator.next()>0) {
        std::cout << "This passage: " << breakiterator.getStartOffset() << ", " << breakiterator.getEndOffset() << std::endl;
    } 
    return "Hello world";
}


// BreakIterator
BreakIterator::BreakIterator(std::string content) {
    startoffset = endoffset = 0;
    content_ = content;
    std::cout << "Content: "  << content << std::endl;
    return;
}

int BreakIterator::getStartOffset() {
    return startoffset;
}

int BreakIterator::getEndOffset() {
    return endoffset;
}

// get to the next 'passage' (next sentence)
int BreakIterator::next() {
// TODO delimit according to sentence
    startoffset = endoffset+1;
    endoffset = startoffset + 10;
    if (startoffset >= content_.size()) 
        return 0;
    if (endoffset >= content_.size())
        endoffset = content_.size()-1;

    return 1; // Success
}
