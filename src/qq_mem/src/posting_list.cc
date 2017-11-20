#include "posting_list.h"
#include <iostream>

PostingList::PostingList(std::string term_in) {
    term_ = term_in;
}


void PostingList::AddPosting(int docID, int term_frequency, const Positions positions) {
    Posting pt(docID, term_frequency, positions);
    postings_[docID] = pt;
}


Posting PostingList::GetPosting(const int &docID) {
    return postings_.at(docID);
}


std::size_t PostingList::Size() {
    return postings_.size();
}


std::string PostingList::serialize() {
    return "";
}

