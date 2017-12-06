#ifndef POSTING_BASIC_H
#define POSTING_BASIC_H

#include "engine_services.h"

// TODO will delete them
typedef int Position;
typedef std::vector<Position> Positions;

class Posting : public PostingService {
    public:
        int docID_;
        int term_frequency_;
        Offsets positions_;

        Posting();
        Posting(int docID, int term_frequency, Offsets offsets_in);
        std::string dump();

        // TODO Posting * next;
};

#endif
