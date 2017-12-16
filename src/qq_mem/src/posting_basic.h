#ifndef POSTING_BASIC_H
#define POSTING_BASIC_H

#include "engine_services.h"
#include <unordered_map>

// TODO will delete them
typedef int Position;
typedef std::vector<Position> Positions;

class Posting : public PostingService {
    public:
        int docID_;
        int term_frequency_;
        Offsets positions_;
        
        Passage_Scores passage_scores_;
        std::unordered_map<int, Passage_Split> passage_splits_; // passage_id to passage_split
 
        Posting();
        Posting(int docID, int term_frequency, Offsets offsets_in);
        std::string dump();

        // TODO Posting * next;
};

#endif
