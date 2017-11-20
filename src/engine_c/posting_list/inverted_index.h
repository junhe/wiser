#ifndef INVERTED_INDEX_H
#define INVERTED_INDEX_H

#include "engine_services.h"

class InvertedIndex: public InvertedIndexService {
    int num_docs;
    public:
        InvertedIndex();
        void AddDocument(int doc_id, TermList termlist);
        void GetPostingList(int doc_id);
        void Search(TermList terms, SearchOperator op);
};


#endif
