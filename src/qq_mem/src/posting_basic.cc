#include "posting_basic.h"

#include <string.h>


//Posting
Posting::Posting(int docID, int term_frequency, const Offsets offsets_in)
    :docID_(docID)
{ // TODO next?
  // TODO precompute passage scores?
    if (FLAG_SNIPPETS_PRECOMPUTE) {
        //parse_offsets_with_precomputing(offsets_in, positions_, passage_scores_);
        positions_ = offsets_in;
        term_frequency_ = term_frequency;
    
    } else {
        positions_ = offsets_in;
        term_frequency_ = term_frequency;
    }
  //passage_scores_ == 
}


Posting::Posting()
    :docID_(0), term_frequency_(0)
{}


std::string Posting::dump() {
    std::string result="-";
    result = result + std::to_string(docID_)+ "_" + std::to_string(term_frequency_);
    for (int i = 0; i < term_frequency_; i++) {
       // result += "_" + std::to_string(positions_[i]);
    }
    return result;
}

