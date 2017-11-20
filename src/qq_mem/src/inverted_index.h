#ifndef INVERTED_INDEX_H
#define INVERTED_INDEX_H

#include "engine_services.h"
#include "posting_list.h"

#include <unordered_map>

class InvertedIndex: public InvertedIndexService {
    protected:
        std::unordered_map<Term, PostingList> index_;

    public:
        void AddDocument(int doc_id, TermList termlist);
        void GetPostingList(int doc_id);
        void Search(TermList terms, SearchOperator op);
};


#endif
