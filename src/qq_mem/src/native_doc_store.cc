#include "native_doc_store.h"

#include <string>
#include <iostream>
#include "unifiedhighlighter.h"  // To change the API

void NativeDocStore::Add(int id, std::string document) {
    if (FLAG_DOCUMENTS_ON_FLASH) {
        // write out to flash
        Store_Segment store_position = Global_Document_Store->append(document);
        // store address in _document_flash_store_
        _document_flash_store_[id] = store_position;
    } else {
        store_[id] = document;
    }
    
    // split into passages
    if (FLAG_SNIPPETS_PRECOMPUTE) {
        Passage_Segments res;
        
        SentenceBreakIterator breakiterator(document);

        // iterate on a string
        int i = 0;
        while ( breakiterator.next() > 0 ) {
            int start_offset = breakiterator.getStartOffset();
            int end_offset = breakiterator.getEndOffset();
            res.push_back(Passage_Segment(start_offset, end_offset-start_offset+1));
        }

        if (FLAG_DOCUMENTS_ON_FLASH) {
            // serialize
            std::stringstream ss; // any stream can be used

            {
              cereal::BinaryOutputArchive oarchive(ss); // Create an output archive
              oarchive(res);
            } // archive goes out of scope, ensuring all contents are flushed

            // write out to flash
            Store_Segment store_position = Global_Document_Store->append(ss.str());
            // store address in _document_flash_store_
            _passage_segments_flash_store_[id] = store_position;
        } else {
            passages_store_[id] = res;
        }
    }
}

void NativeDocStore::Remove(int id) {
    store_.erase(id);
}

bool NativeDocStore::Has(int id) {
    return store_.count(id) == 1;
}

const std::string & NativeDocStore::Get(int id) {
    if (FLAG_DOCUMENTS_ON_FLASH) {
        if (_document_cache_.exists(id)) {
            return _document_cache_.get(id);
        }
        //std::cout << "--IO-- fetch document from flash" << std::endl;
        // get postion
        const Store_Segment stored_position = _document_flash_store_[id];
        // read -> add to cache
        _document_cache_.put(id, Global_Document_Store->read(stored_position));
        // return document        TODO Lock Problems(should not kick out a document from cache while still using it)
        return _document_cache_.get(id);
    }
    return store_[id];
}

// for precompute
const Passage_Segments & NativeDocStore::GetPassages(int id) {
    if (FLAG_DOCUMENTS_ON_FLASH) {
        if (_passage_segments_cache_.exists(id)) {
            return _passage_segments_cache_.get(id);
        }
        //std::cout << "--IO-- fetch passage segment from flash" << std::endl;
        // get postion
        const Store_Segment stored_position = _passage_segments_flash_store_[id];
        // read -> add to cache
        _passage_segments_cache_.put(id, parse_cereal_string_to_passage_segments(Global_Document_Store->read(stored_position)));
        // return passage segments        TODO Lock Problems(should not kick out a document from cache while still using it)
        return _passage_segments_cache_.get(id);
    }
    return passages_store_[id];
}

const Passage_Segments NativeDocStore::parse_cereal_string_to_passage_segments(const std::string & serialized) {
    std::stringstream ss;
    ss.str(serialized);

    cereal::BinaryInputArchive iarchive(ss); // Create an input archive

    Passage_Segments new_passage_segments;
    iarchive(new_passage_segments); // Read the data from the archive
    
    return new_passage_segments;
}

void NativeDocStore::Clear() {
    store_.clear();
}

int NativeDocStore::Size() {
    return store_.size();
}


