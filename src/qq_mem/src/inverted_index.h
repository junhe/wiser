#ifndef INVERTED_INDEX_H
#define INVERTED_INDEX_H

#include <unordered_map>

#include "engine_services.h"
#include "types.h"
#include "posting_list_direct.h"
#include "lrucache.h"
#include <cereal/archives/binary.hpp>
#include <sstream>

typedef std::unordered_map<Term, PostingList_Direct> IndexStore;

class InvertedIndex: public InvertedIndexService {
    protected:
        IndexStore index_;
        /* For flash-based QQ,
           1. cache for postings
           2. flash reader for flash-based store
        */
        cache::lru_cache<std::string, Posting> _postings_cache_ {cache::lru_cache<std::string, Posting>(POSTINGS_CACHE_SIZE)};
    public:
        typedef IndexStore::const_iterator const_iterator;

        void AddDocument(const int &doc_id, const TermList &termlist);
        void AddDocument(const int &doc_id, const TermWithOffsetList &termlist);
        std::vector<int> GetDocumentIds(const Term &term);
        std::vector<int> Search(const TermList &terms, const SearchOperator &op);
        const Posting & GetPosting(const Term & term, const int & doc_id);
        void clear_posting_cache() {_postings_cache_.clear();}
        IndexStore::const_iterator Find(const Term &term);
        IndexStore::const_iterator ConstEnd();
    
    private:
        std::string construct_key(const Term & term, const int & docID); // helper function for _postings_cache_
        const Posting parse_protobuf_string_to_posting(const std::string & serialized);
        const Posting parse_cereal_string_to_posting(const std::string & serialized);
};

#endif
