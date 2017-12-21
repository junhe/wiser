#ifndef POSTING_BASIC_H
#define POSTING_BASIC_H

#include "engine_services.h"

typedef int Position;
typedef std::vector<Position> Positions;

class Posting : public PostingService {
    public:
        int docID_;
        int term_frequency_;
        Positions positions_;

        Posting();
        Posting(int docID, int term_frequency, Positions positions);
        std::string dump();
        const int GetDocID() const {return docID_;}
        const int GetTermFreq() const {return term_frequency_;}
        const Positions GetPositions() const {return positions_;}

        // TODO Posting * next;
};

#endif
