#include "unifiedhighlighter.h"
#include <iostream>
#include <queue>

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
    res.push_back(Offset_Iterator(test_offsets_1));
    res.push_back(Offset_Iterator(test_offsets_2));
    res.push_back(Offset_Iterator(test_offsets_3));

    /*for (Query::iterator it = query.begin(); it != query.end(); ++ it) {
        // push back iterator of offsets for this term in this document
    } */
    return res; 
}



std::string UnifiedHighlighter::highlightOffsetsEnums(OffsetsEnums & offsetsEnums, const int & docID) {
    // break the document according to sentence
    SentenceBreakIterator breakiterator(engine_.GetDocument(docID));
    
    // merge sorting
    // priority queue for OffsetIterator
    auto comp_offset = [] (Offset_Iterator &a, Offset_Iterator &b) -> bool { return a.startoffset > b.startoffset; };
    std::priority_queue<Offset_Iterator, std::vector<Offset_Iterator>, decltype(comp_offset) > offsets_queue(comp_offset);
    for (OffsetsEnums::iterator it = offsetsEnums.begin(); it != offsetsEnums.end(); it++ ) {
        offsets_queue.push(*it);
        std::cout << it->startoffset << " ";
    }
    std::cout << " End" << std::endl;
    std::cout << " Test: ";
    while (!offsets_queue.empty()) {
        std::cout << offsets_queue.top().startoffset << " ";
        offsets_queue.pop();
    }
    std::cout << std::endl;

    // priority queue for passages
    auto comp_passage = [] (Passage &a, Passage &b) -> bool { return a.score > b.score; };
    std::priority_queue<Passage, std::vector<Passage>, decltype(comp_passage) > passage_queue(comp_passage);

/*
    Passage passage();
    passage.startoffset = ;
    passage.endoffset = ;
 
    while (!offsets_queue.empty()) {
        // analyze current passage
        
        poll a offset iterator
        // judge whether this iterator is over

        // judge whether this iterator's offset is beyond current passage

        // Add this term to current passage until out of this passage
        

    }  
 
    while (breakiterator.next()>0) {
        //std::cout << "This passage: " << breakiterator.getStartOffset() << ", " << breakiterator.getEndOffset() << std::endl;
    }
*/    
    return "Hello world";
}

Offset_Iterator::Offset_Iterator(std::vector<Offset> & offsets_in) {
    offsets = &offsets_in;
    cur_position = offsets_in.begin(); 
    startoffset = std::get<0>(*cur_position);
    endoffset = std::get<1>(*cur_position);
}

void Offset_Iterator::next_position() {  // go to next offset position
    cur_position++;
    if (cur_position == offsets->end()) {
        startoffset = endoffset = -1;
    } 
    startoffset = std::get<0>(*cur_position);
    endoffset = std::get<1>(*cur_position);
}

// BreakIterator
SentenceBreakIterator::SentenceBreakIterator(std::string content) {
    startoffset = endoffset = -1;
    content_ = content;
    // start boost boundary analysis
    boost::locale::boundary::sboundary_point_index tmp(boost::locale::boundary::sentence, content_.begin(), content_.end(),gen("en_US.UTF-8"));
    map = tmp;
    map.rule(boost::locale::boundary::sentence_term);
    
    current = map.begin();
    last = map.end();
    last_offset = content.size()-1;

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
    if (endoffset >= last_offset) {
        return 0;
    }
 
    ++current;
    std::string::const_iterator this_offset = *current;

    startoffset = endoffset + 1;
    endoffset = this_offset - content_.begin() - 1;
    return 1; // Success
}
