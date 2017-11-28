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
    SentenceBreakIterator breakiterator(engine_.GetDocument(docID));
    // traverse all sentences and print
    // merge sorting
    while (breakiterator.next()>0) {
        //std::cout << "This passage: " << breakiterator.getStartOffset() << ", " << breakiterator.getEndOffset() << std::endl;
    }
    
    return "Hello world";
}


// BreakIterator
SentenceBreakIterator::SentenceBreakIterator(std::string content) {
    startoffset = endoffset = 0;
    content_ = content;
    std::cout << "Content: "  << content << std::endl;
    // start boost boundary analysis
    boost::locale::boundary::sboundary_point_index tmp(boost::locale::boundary::sentence, content_.begin(), content_.end(),gen("en_US.UTF-8"));
    map = tmp;
    map.rule(boost::locale::boundary::sentence_term);
    current = map.begin(), last = map.end();
    
    return;
}

int SentenceBreakIterator::getStartOffset() {
    return startoffset;
}

int SentenceBreakIterator::getEndOffset() {
    return endoffset;
}

// get to the next 'passage' (next sentence)
int SentenceBreakIterator::next() {
// TODO delimit according to sentence
    if (current == last) {
        return 0;
    }

    ++current;
    std::string::const_iterator this_offset = *current;
    std::cout << this_offset - content_.begin() << std::endl;

    startoffset = endoffset + 1;
    endoffset = this_offset - content_.begin();
    return 1; // Success
}
