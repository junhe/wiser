#ifndef NATIVE_DOC_STORE_HH
#define NATIVE_DOC_STORE_HH

#include <map>
#include <cereal/types/vector.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/archives/binary.hpp>
#include "lrucache.h"

#include "engine_services.h"
#include "types.h"

class NativeDocStore: public DocumentStoreService {
    private:
        std::map<int,std::string> store_;
        std::map<int,Passage_Segments> passages_store_;
        // For flash_based document store: 
         // cache for document and document passages   (docid, string) or (docid, Passage_Segments)
        cache::lru_cache<int, std::string> _document_cache_ {cache::lru_cache<int, std::string>(DOCUMENTS_CACHE_SIZE)};
        cache::lru_cache<int, Passage_Segments> _passage_segments_cache_ {cache::lru_cache<int, Passage_Segments>(DOCUMENTS_CACHE_SIZE)};
         // store positions of document and passages on flash
        std::unordered_map<int, Store_Segment> _document_flash_store_;
        std::unordered_map<int, Store_Segment> _passage_segments_flash_store_;

        const Passage_Segments parse_cereal_string_to_passage_segments(const std::string & serialized);

    public:
        NativeDocStore() {}
        void clear_caches() {_document_cache_.clear(); _passage_segments_cache_.clear();}

        void Add(int id, std::string document);
        void Remove(int id);
        const std::string & Get(int id);
        const Passage_Segments & GetPassages(int id);
        bool Has(int id);
        void Clear();
        int Size();
};

#endif
