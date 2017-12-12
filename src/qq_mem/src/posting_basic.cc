#include "posting_basic.h"

#include <string.h>
#include <iostream>

//Posting
Posting::Posting(int docID, int term_frequency, const Offsets offsets_in)
    :docID_(docID), positions_(offsets_in), term_frequency_(term_frequency)
{ // TODO next?
  // get precomputed passage scores
    if (FLAG_SNIPPETS_PRECOMPUTE) {
        // find (-1, -1)  = cur_pos - 1
        int cur_pos = offsets_in.size() - std::get<0>(offsets_in.back()) - 1 - 1;
        while (std::get<0>(offsets_in[cur_pos]) != -1) {
            cur_pos++;
        }
        cur_pos++;

        // get positions
        positions_.assign(offsets_in.begin(), offsets_in.begin() + cur_pos-2);
        term_frequency_ = cur_pos-1;
        
        // get precomputed scores
        for (; cur_pos < offsets_in.size(); cur_pos++) {
            float score = (float)(std::get<1>(offsets_in[cur_pos])) / 1000000000;
            std::cout << "Posting get score: " << score << std::endl;
            passage_scores_.push_back(Passage_Score(std::get<0>(offsets_in[cur_pos]), score));
        }
    }
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

