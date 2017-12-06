#include "posting_basic.h"

#include <string.h>


//Posting
Posting::Posting(int docID, int term_frequency, const Offsets offsets_in)
    :docID_(docID), term_frequency_(term_frequency), positions_(offsets_in)
{ // TODO next?
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

