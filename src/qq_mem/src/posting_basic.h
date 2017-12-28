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
        const int GetDocId() const {return docID_;}
        const int GetTermFreq() const {return term_frequency_;}
        const Positions GetPositions() const {return positions_;}

        // TODO Posting * next;
};


class RankingPosting {
  public:
    int doc_id_;
    int term_frequency_;

    RankingPosting(){};
    RankingPosting(int doc_id, int term_frequency)
      :doc_id_(doc_id), term_frequency_(term_frequency)
    {
    }
    const int GetDocId() const {return doc_id_;}
    const int GetTermFreq() const {return term_frequency_;}
};


#endif
