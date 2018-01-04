#include "unifiedhighlighter.h"
#include "posting_basic.h"
#include <iostream>
#include <queue>
#include <algorithm>

// UnifiedHighlighter Functions
UnifiedHighlighter::UnifiedHighlighter(QQSearchEngine & engine) {
    engine_ = & engine;
    //_snippets_on_flash_.open("snippets_cache");
}

UnifiedHighlighter::UnifiedHighlighter() {
    // for test
}

std::vector<std::string> UnifiedHighlighter::highlight(const Query & query, const TopDocs & topDocs, const int & maxPassages) {
    std::vector<std::string> res = {}; // size of topDocs
    for(TopDocs::const_iterator cur_doc = topDocs.begin(); cur_doc != topDocs.end(); ++cur_doc) {
        // TODO multi-thread for multi docs
        std::string cur_snippet = highlightForDoc(query, *cur_doc, maxPassages);  // finally all Passages will be formated into one string
        res.push_back( cur_snippet );
    }

    return res;
}

std::string UnifiedHighlighter::highlightForDoc(const Query & query, const int & docID, const int &maxPassages) {
    // check cache
    std::string this_key = "";
    if (FLAG_SNIPPETS_CACHE) { 
        this_key = construct_key(query, docID);
        if (_snippets_cache_.exists(this_key)) {
           return _snippets_cache_.get(this_key);
        }
    }

    // check flash cache
    if (FLAG_SNIPPETS_CACHE_ON_FLASH) {
        this_key = construct_key(query, docID);
        if (_snippets_cache_flash_.exists(this_key)) {
           std::cout << "Hit in flash cache " << std::endl;
           return _snippets_cache_flash_.get(this_key);
        }
    }

    // Real Work: highlight according to offset iterators and content of this document
    std::string res;
    if (FLAG_SNIPPETS_PRECOMPUTE)
        res = highlightQuickForDoc(query, docID, maxPassages);
    else 
        res = highlightOffsetsEnums(getOffsetsEnums(query, docID), docID, maxPassages);

    // update cache
    if (FLAG_SNIPPETS_CACHE) {
        _snippets_cache_.put(this_key, res);
    }

    // update flash cache
    if (FLAG_SNIPPETS_CACHE_ON_FLASH) {
        std::cout << "Add to flash cache " << this_key << " size: " << res.size() << std::endl;
        _snippets_cache_flash_.put(this_key, res);
    }

    return res;
}

OffsetsEnums UnifiedHighlighter::getOffsetsEnums(const Query & query, const int & docID) {
    OffsetsEnums res = {};
    // TODO set weight of each term 

    // get offsets from postings
    for (auto term:query) {
        // Posting
        const Posting & result = engine_->inverted_index_.GetPosting(term, docID);
        // Offsets
        res.push_back(Offset_Iterator(result.positions_));
        // TODO check whether empty?
    }
    return res;
}

std::vector<int> UnifiedHighlighter::get_top_passages(const ScoresEnums & scoresEnums, const int & maxPassages) { // TODO
    bool FLAG_PAAT = true;   // PAAT means passage at a term algorithm; TAAT means term as a time
    std::vector<int> res = {};

    if (FLAG_PAAT) {
        // priority queue for passage_score iterators, holding the first passage IDs
        auto comp_passage_id = [] ( PassageScore_Iterator &a, PassageScore_Iterator &b ) -> bool { return a.cur_passage_id_ > b.cur_passage_id_; };
        std::priority_queue<PassageScore_Iterator, std::vector<PassageScore_Iterator>, decltype(comp_passage_id)> scores_iterator_queue(comp_passage_id);
        for (auto scores_iter:scoresEnums) {
            scores_iterator_queue.push(scores_iter);
        }
        
        // priority queue for passages, holding top passages
        auto comp_passage = [] (std::pair<int, float> & a, std::pair<int, float> & b) -> bool { return a.second > b.second; };
        std::priority_queue<std::pair<int, float>, std::vector<std::pair<int, float>>, decltype(comp_passage)> passage_queue(comp_passage); 

        // merge
        std::pair<int, float> passage(-1, 0);  // current passage (id, score)
        while (!scores_iterator_queue.empty()) {
            PassageScore_Iterator cur_iter = scores_iterator_queue.top();
            scores_iterator_queue.pop();  // push_back
           
            int next_passage_id = cur_iter.cur_passage_id_;
            if (next_passage_id == -1)
                continue;

            // if not same with cur_passage
            if (next_passage_id != passage.first) {
                if (passage.first != -1) {
                    if (passage_queue.size() == maxPassages && passage.second <= passage_queue.top().second) {
                        passage.first = -1; passage.second = 0;
                    } else {
                        passage_queue.push(passage);
                        if (passage_queue.size() > maxPassages) {
                            passage = passage_queue.top();
                            passage_queue.pop();
                        } 
                        passage.first = -1; passage.second = 0;
                    }
                }
                // advance to next passage
                passage.first = next_passage_id;
            }
            passage.second = passage.second + cur_iter.weight * cur_iter.score_;
            // push back this scores iterator
            cur_iter.next_passage();
            scores_iterator_queue.push(cur_iter);
        }

        // Add the last passage
        if (passage_queue.size() < maxPassages && passage.second > 0) {
            passage_queue.push(passage);
        } else {
            if (passage.second > passage_queue.top().second) {
                passage_queue.pop();
                passage_queue.push(passage);
            }
        }

        // format it into a string to return
        while (!passage_queue.empty()) {
            //std::cout << passage_queue.top().first << "," << passage_queue.top().second << ";"; 
            res.push_back(passage_queue.top().first);
            passage_queue.pop();
        }
        //std::cout << std::endl;
    } else {
    

    }
    return res;
}

ScoresEnums UnifiedHighlighter::get_passages_scoresEnums(const Query & query, const int & docID) {
    ScoresEnums res = {};

    // get passage_scores from postings
    for (auto term:query) {
        // Posting
        const Posting & result = engine_->inverted_index_.GetPosting(term, docID);
        // Offsets
        res.push_back(PassageScore_Iterator(result.passage_scores_));
    }
    return res;
    
}

std::string UnifiedHighlighter::highlight_passages(const Query & query, const int & docID, const std::vector<int> & top_passages) {
    // get the offset of
    Passage cur_passage;
    std::string res = "";
    std::vector<int> passages = top_passages;
    std::sort(passages.begin(), passages.end());
    for (auto passage_id : passages) {
        cur_passage.reset();
        // add matches for highlighting
        for (auto term:query) {
            const Posting & cur_posting = engine_->inverted_index_.GetPosting(term, docID);
            int startoffset = cur_posting.passage_splits_.at(passage_id).first;
            int len = cur_posting.passage_splits_.at(passage_id).second;
            for (int i = 0; i < len; i++) {
                cur_passage.addMatch(std::get<0>(cur_posting.positions_[startoffset+i]), std::get<1>(cur_posting.positions_[startoffset+i]));
            }
        }
        // get the string of this passage
        // get offsets
        cur_passage.startoffset = engine_->doc_store_.GetPassages(docID)[passage_id].first;
        cur_passage.endoffset = cur_passage.startoffset - 1 + engine_->doc_store_.GetPassages(docID)[passage_id].second;
        // get passage
        res += cur_passage.to_string(&engine_->doc_store_.Get(docID));
    }
    return res;
}


std::string UnifiedHighlighter::highlightQuickForDoc(const Query & query, const int & docID, const int &maxPassages) {
    /* precomputed:
           1. passage sentence splits in DocumentStoreService.passages_store_
           2. each item, each posting: every passage score for this document in posting_basic.passage_scores_
       to compute:
           1. top-maxPassages scores of those passages for (query, docID)
    */
    // Get top passages
    std::vector<int> top_passages = get_top_passages(get_passages_scoresEnums(query, docID), maxPassages);
    // Highlight words
    std::string res = highlight_passages(query, docID, top_passages);
    return res;
}

std::string UnifiedHighlighter::highlightOffsetsEnums(const OffsetsEnums & offsetsEnums, const int & docID, const int & maxPassages) {
    // break the document according to sentence
    SentenceBreakIterator breakiterator(engine_->GetDocument(docID));
    
    // "merge sorting" to calculate all sentence's score
    
    // priority queue for Offset_Iterator TODO extra copy
    auto comp_offset = [] (Offset_Iterator &a, Offset_Iterator &b) -> bool { return a.startoffset > b.startoffset; };
    std::priority_queue<Offset_Iterator, std::vector<Offset_Iterator>, decltype(comp_offset)> offsets_queue(comp_offset);
    for (auto it = offsetsEnums.begin(); it != offsetsEnums.end(); it++ ) {
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
        // judge whether this iterator is over
        if (cur_start == -1)
            continue;

        // judge whether this iterator's offset is beyond current passage
        if (cur_start >= passage->endoffset) {
            // if this passage is not empty, then wrap up it and push it to the priority queue
            if (passage->startoffset >= 0) {
                passage->score = passage->score * passage_norm(passage->startoffset); //normalize according to passage's startoffset
                if (passage_queue.size() == maxPassages && passage->score <= passage_queue.top()->score) {
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
        passage->score = passage->score + cur_iter.weight * tf_norm(tf, passage->endoffset-passage->startoffset+1);   //scoring
    }

    // Add the last passage
    passage->score = passage->score * passage_norm(passage->startoffset);
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
    for (auto it = passage_vector.begin(); it != passage_vector.end(); it++) {
        res += (*it)->to_string(breakiterator.content_);   // highlight appeared terms
        //std::cout <<  "("  << (*it)->score << ") " << breakiterator.content_->substr((*it)->startoffset, (*it)->endoffset - (*it)->startoffset+1) << std::endl;
        delete (*it);
    }
    
    return res;
}

float UnifiedHighlighter::passage_norm(const int & start_offset) {
    return 1 + 1/(float) log((float)(pivot + start_offset));
}

float UnifiedHighlighter::tf_norm(const int & freq, const int & passageLen) {
    float norm = k1 * ((1 - b) + b * (passageLen / pivot));
    return freq / (freq + norm);
}

std::string UnifiedHighlighter::construct_key(const Query & query, const int & docID) {
    std::string res = std::to_string(docID) + ":";
    for (auto term:query) {
        res += term + ",";
    }
    return res;
}


