#ifndef POSTING_BASIC_H
#define POSTING_BASIC_H

#include "engine_services.h"
#include <unordered_map>

// TODO will delete them
typedef int Position;
typedef std::vector<Position> Positions;

class Posting : public PostingService {
    public:
        int docID_ = 0;
        int term_frequency_ = 0;
        Offsets positions_ = {};
        
        Passage_Scores passage_scores_ = {};
        std::unordered_map<int, Passage_Split> passage_splits_; // passage_id to passage_split
 
        Posting();
        Posting(const int & docID, const int & term_frequency, const Offsets & offsets_in);
        Posting(const int & docID, const int & term_frequency, 
                Offsets * offsets_in, Passage_Scores * passage_scores, std::unordered_map<int, Passage_Split> * passage_splits);
        std::string dump();

        // TODO Posting * next;
};

#endif
