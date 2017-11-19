#ifndef INVERTED_INDEX_HH
#define INVERTED_INDEX_HH

#include "engine_services.h"
#include "posting_list.h"

#include <unordered_map>

typedef std::unordered_map<Term, PostingList> IndexStore;

class InvertedIndex: public InvertedIndexService {
    protected:
        IndexStore index_;

    public:
        void AddDocument(const int &doc_id, const TermList &termlist);
        void GetPostingList(const int &doc_id);
        void Search(const TermList &terms, const SearchOperator &op);
};


#endif
