#ifndef INVERTED_INDEX_H
#define INVERTED_INDEX_H

#include "engine_services.h"
#include "posting_list_direct.h"

#include <unordered_map>

typedef std::unordered_map<Term, PostingList_Direct> IndexStore;

class InvertedIndex: public InvertedIndexService {
    protected:
        IndexStore index_;

    public:
        void AddDocument(const int &doc_id, const TermList &termlist);
        std::vector<int> GetDocumentIds(const Term &term);
        std::vector<int> Search(const TermList &terms, const SearchOperator &op);
};

#endif
