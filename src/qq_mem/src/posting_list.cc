#include "posting_list.h"

#include <iostream>
#include <string>

Posting::Posting(int docID, int term_frequency, const Positions positions)
    :docID_(docID), term_frequency_(term_frequency), positions_(positions)
{}


Posting::Posting()
    :docID_(0), term_frequency_(0)
{}


std::string Posting::dump() {
    return std::to_string(docID_);
}


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


std::string PostingList::Serialize() {
    return "";
}

