#include "posting_list_direct.h"
#include <iostream>

// Init with a term string, when creating index
PostingList_Direct::PostingList_Direct(std::string term_in) {
    term_ = term_in;
    num_postings = 0;
    p_list = {};
    cur_index = -1;
    cur_docID = -1;
}

// Return size
std::size_t PostingList_Direct::Size() {
    return num_postings;
}

// Get next posting
Posting PostingList_Direct::GetPosting() {  // exactly next
    return GetPosting(cur_docID+1);
} 

Posting PostingList_Direct::GetPosting(const int &next_doc_ID) {   // next Posting whose docID>next_doc_ID
    while (cur_docID < next_doc_ID) {
        if (cur_index == num_postings-1)  // get the end
            return p_list[cur_index];     // TODO throw exception
        cur_index++;
        cur_docID = p_list[cur_index].docID_;
    }
    return p_list[cur_index];
}


// Add a doc for creating index
void PostingList_Direct::AddPosting(int docID, int term_frequency, const Positions positions) {
    std::cout << "Add document " << docID << " for " << term_ << std::endl;
    // add one posting to the p_list
    num_postings += 1;
    Posting new_posting(docID, term_frequency, positions);
    p_list.push_back( new_posting );  //TODO
    return;
}

std::string PostingList_Direct::serialize() {
    return "";
}
