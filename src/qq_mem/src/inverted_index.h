#ifndef INVERTED_INDEX_H
#define INVERTED_INDEX_H

#include <unordered_map>

#include "engine_services.h"
#include "posting_list_direct.h"
#include "lru_cache.h"

typedef std::unordered_map<Term, PostingList_Direct> IndexStore;

class InvertedIndex: public InvertedIndexService {
    protected:
        IndexStore index_;
        // For flash-based QQ, cache for postings
        cache::lru_cache<std::string, Posting> _postings_cache_ {cache::lru_cache<std::string, Posting>(POSTINGS_CACHE_SIZE)};
        FlashReader _postings_flash_store_ {FlashReader(POSTINGS_ON_FLASH_FILE)}; 
    public:
        void AddDocument(const int &doc_id, const TermList &termlist);
        void AddDocument(const int &doc_id, const TermWithOffsetList &termlist);
        std::vector<int> GetDocumentIds(const Term &term);
        std::vector<int> Search(const TermList &terms, const SearchOperator &op);
        Posting & GetPosting(const Term & term, const int & doc_id);
};

#endif
