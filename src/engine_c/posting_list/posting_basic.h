#ifndef POSTING_BASIC_H
#define POSTING_BASIC_H

#include "engine_services.h"

class Posting : public PostingStructure {
    public:
    int docID;
    int term_frequency;
    int * position;

    Posting * next;

    Posting(int docID_in, int term_frequency_in, int position_in[]); 
    ~Posting();
    std::string dump();
};

#endif
