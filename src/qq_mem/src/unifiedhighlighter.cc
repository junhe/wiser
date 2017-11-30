#include "unifiedhighlighter.h"
#include <iostream>
#include <queue>
#include <algorithm>

UnifiedHighlighter::UnifiedHighlighter(QQSearchEngine & engine) {
    engine_ = engine;
}

UnifiedHighlighter::UnifiedHighlighter() {
    // for test
}

std::vector<std::string> UnifiedHighlighter::highlight(Query & query, TopDocs & topDocs, int & maxPassages) {
    std::vector<std::string> res = {}; // size of topDocs
    for(TopDocs::iterator cur_doc = topDocs.begin(); cur_doc != topDocs.end(); ++cur_doc) {
        std::string cur_snippet = highlightForDoc(query, *cur_doc, maxPassages);  // finally all Passages will be formated into one string
        std::cout << "generated for " << *cur_doc << " result: " << cur_snippet << std::endl;
        res.push_back( cur_snippet );
    }
    return res;
}



std::string UnifiedHighlighter::highlightForDoc(Query & query, const int & docID, int &maxPassages) {
    // get offset iterators for each query term for this document
    OffsetsEnums offsetsEnums = getOffsetsEnums(query, docID);
    // highlight according to offset iterators and content of this document
    std::string res = highlightOffsetsEnums(offsetsEnums, docID, maxPassages);
    
    return res;
}

OffsetsEnums UnifiedHighlighter::getOffsetsEnums(Query & query, const int & docID) {
    //
    OffsetsEnums res = {};

    // TODO set weight of each term 

    res.push_back(Offset_Iterator(test_offsets_1));
    res.push_back(Offset_Iterator(test_offsets_2));
    res.push_back(Offset_Iterator(test_offsets_3));

    /*for (Query::iterator it = query.begin(); it != query.end(); ++ it) {
        // push back iterator of offsets for this term in this document
    } */
    return res; 
}



std::string UnifiedHighlighter::highlightOffsetsEnums(OffsetsEnums & offsetsEnums, const int & docID, int & maxPassages) {
    // break the document according to sentence
    SentenceBreakIterator breakiterator(engine_.GetDocument(docID));
    
    // "merge sorting" to calculate all sentence's score
    
    // priority queue for OffsetIterator
    auto comp_offset = [] (Offset_Iterator &a, Offset_Iterator &b) -> bool { return a.startoffset > b.startoffset; };
    std::priority_queue<Offset_Iterator, std::vector<Offset_Iterator>, decltype(comp_offset)> offsets_queue(comp_offset);
    for (OffsetsEnums::iterator it = offsetsEnums.begin(); it != offsetsEnums.end(); it++ ) {
        offsets_queue.push(*it);
    }
    // priority queue for passages
    auto comp_passage = [] (Passage * & a, Passage * & b) -> bool { return a->score > b->score; };
    std::priority_queue<Passage *, std::vector<Passage*>, decltype(comp_passage)> passage_queue(comp_passage);

    // start caculate score for passage
    Passage * passage = new Passage();  // current passage

    while (!offsets_queue.empty()) {
        // analyze current passage
        Offset_Iterator cur_iter = offsets_queue.top();
        offsets_queue.pop();

        int cur_start = cur_iter.startoffset;
        int cur_end = cur_iter.endoffset;
        //std::cout << "handling: " << cur_start << ", " << cur_end << std::endl;
        // judge whether this iterator is over
        if (cur_start == -1)
            continue;

        // judge whether this iterator's offset is beyond current passage
        if (cur_start >= passage->endoffset) {
            // if this passage is not empty, then wrap up it and push it to the priority queue
            if (passage->startoffset >= 0) {
                passage->score = passage->score * 1; //TODO normalize according to passage's startoffset
                if (passage_queue.size() == maxPassages && passage->score < passage_queue.top()->score) {
                    passage->reset();
                } else {
                    passage_queue.push(passage);
                    if (passage_queue.size() > maxPassages) {
                        passage = passage_queue.top();
                        passage_queue.pop();
                        passage->reset();
                    } else {
                        passage = new Passage();
                    }
                }
            }
            // advance to next passage
            if (breakiterator.next(cur_start+1) <= 0)
                break;
            passage->startoffset = breakiterator.getStartOffset();
            passage->endoffset = breakiterator.getEndOffset();
        }

        // Add this term's appearance to current passage until out of this passage
        int tf = 0;
        while (1) {
            tf++;
            //std::cout << "Add: " << cur_start << ", " <<cur_end <<std::endl;
            passage->addMatch(cur_start, cur_end); 
            cur_iter.next_position();
            if (cur_iter.startoffset == -1) {
                break;
            }
            cur_start = cur_iter.startoffset;
            cur_end = cur_iter.endoffset;
            if (cur_start >= passage->endoffset) {
                offsets_queue.push(cur_iter);
                break;
            }
        }
        // Add the score from this term to this passage
        passage->score = passage->score + 1 * tf;   //TODO scoring: weight
    }

    // Add the last passage
    if (passage_queue.size() < maxPassages && passage->score > 0) {
        passage_queue.push(passage);
    } else {
        if (passage->score > passage_queue.top()->score) {
            Passage * tmp = passage_queue.top();
            passage_queue.pop();
            delete tmp;
            passage_queue.push(passage);
        }
    }

    // format it into a string to return
    // dump the passages
    // transform to array
    std::vector<Passage *> passage_vector;
    while (!passage_queue.empty()) {
        passage_vector.push_back(passage_queue.top());
        passage_queue.pop();
    }
    // sort array according to startoffset
    auto comp_passage_offset = [] (Passage * & a, Passage * & b) -> bool { return a->startoffset < b->startoffset; };
    std::sort(passage_vector.begin(), passage_vector.end(), comp_passage_offset);
    
    // format the final string
    std::string res = "";
    std::cout << breakiterator.content_ << std::endl;
    for (auto it = passage_vector.begin(); it != passage_vector.end(); it++) {
        res += (*it)->to_string(breakiterator.content_);   // highlight appeared terms
        std::cout <<  "("  << (*it)->score << ") " << breakiterator.content_.substr((*it)->startoffset, (*it)->endoffset - (*it)->startoffset+1) << std::endl;
        delete (*it);
    }
    std::cout << "Result:\n" << res << "\nEND" << std::endl;
    
    return res;
}


// Offset_Iterator Functions
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
        return ;
    } 
    startoffset = std::get<0>(*cur_position);
    endoffset = std::get<1>(*cur_position);
    return;
}


//Passage 
std::string Passage::to_string(std::string & doc_string) {
    //TODO highlight
    std::string res= "";
    res += doc_string.substr(startoffset, endoffset - startoffset + 1) + "\n";
    
    // highlight
    auto cmp_terms = [] (Offset & a, Offset & b) -> bool { return (std::get<0>(a) > std::get<0>(b));};
    std::sort(matches.begin(), matches.end(), cmp_terms);

    for (auto it = matches.begin(); it != matches.end(); it++) {
        res.insert(std::get<1>(*it)-startoffset+1, "<\\b>");
        res.insert(std::get<0>(*it)-startoffset, "<b>");
    }
    return res;
}


// BreakIterator Functions
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

// get to the next 'passage' contains offset
int SentenceBreakIterator::next(int offset) {
    if (offset >=last_offset) {
        return 0;
    }
    current = map.find(content_.begin() + offset);
    std::string::const_iterator this_offset = *current;
    
    endoffset = this_offset - content_.begin() - 1;
    current--;
    std::string::const_iterator tmp_offset = *current;
    startoffset = tmp_offset - content_.begin() - 1 + 1;
    current++;
    return 1; // Success
}


// Passage Funcitons

void Passage::addMatch(int & startoffset, int & endoffset) {
   matches.push_back(std::make_tuple(startoffset, endoffset)); 
   return ;
} 

