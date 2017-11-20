#ifndef POSTING_LIST_H
#define POSTING_LIST_H

#include <string>
#include <vector>

#include "engine_services.h"


typedef int Position;
typedef std::vector<Position> Positions;

class Posting {
    public:
        int docID_;
        int term_frequency_;
        Positions positions_;

        Posting();
        Posting(int docID, int term_frequency, Positions positions);
        std::string dump();
};

class PostingList {
    Term term_;
    std::map<int, Posting> postings_;
    
    public:
        PostingList(std::string term_in);
        void AddPosting(int docID, int term_frequency, const Positions positions);
        Posting GetPosting(const int &docID);
        std::size_t Size();
        std::string serialize();
};


#endif
