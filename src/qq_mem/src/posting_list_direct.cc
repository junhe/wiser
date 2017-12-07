#include "posting_list_direct.h"
#include <iostream>

// Init with a term string, when creating index
PostingList_Direct::PostingList_Direct(std::string term_in): 
    term_(term_in) 
{}

// Return size
std::size_t PostingList_Direct::Size() {
    return posting_store_.size();
}

// Add a doc for creating index
void PostingList_Direct::AddPosting(int docID, int term_frequency, const Offsets positions) {
    Posting new_posting(docID, term_frequency, positions);
    posting_store_[docID] = new_posting;
    return;
}

// Customized iterators are hard to implement, so we just 
// Use this function to extract posting from the iterator
Posting PostingList_Direct::ExtractPosting(const_iterator it) {
    return it->second;
}

std::string PostingList_Direct::Serialize() {
    return "";
}

// Find a posting according to docID
Posting & PostingList_Direct::GetPosting(const int & docID) {
    //auto search = posting_store_.find(docID);
    return (posting_store_.find(docID))->second;
}
