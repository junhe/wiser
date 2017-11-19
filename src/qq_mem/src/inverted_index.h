#ifndef INVERTED_INDEX_HH
#define INVERTED_INDEX_HH

#include "engine_services.h"

class InvertedIndex: public InvertedIndexService {
    public:
        void AddDocument(int doc_id, TermList termlist);
        void GetPostingList(int doc_id);
        void Search(TermList terms, SearchOperator op);
};


#endif
