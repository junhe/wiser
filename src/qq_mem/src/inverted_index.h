#ifndef INVERTED_INDEX_H
#define INVERTED_INDEX_H

#include "engine_services.h"
#include "posting_list_direct.h"

#include <unordered_map>

class InvertedIndex: public InvertedIndexService {
    protected:
        std::unordered_map<Term, PostingList_Direct> index_;

    public:
        void AddDocument(int doc_id, TermList termlist);
        void GetPostingList(int doc_id);
        void Search(TermList terms, SearchOperator op);
};


#endif
