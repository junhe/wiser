#ifndef INVERTED_INDEX_HH
#define INVERTED_INDEX_HH

#include "engine_services.h"

#include <unordered_map>

class InvertedIndex: public InvertedIndexService {
    // protected:
        // std::unordered_map<Term, PostingListService> index_;

    public:
        void AddDocument(const int &doc_id, const TermList &termlist);
        void GetPostingList(const int &doc_id);
        void Search(const TermList &terms, const SearchOperator &op);
};


#endif
