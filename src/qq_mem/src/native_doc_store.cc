#include "native_doc_store.h"

#include <string>
#include <iostream>
#include "unifiedhighlighter.h"  // To change the API

void NativeDocStore::Add(int id, std::string document) {
    store_[id] = document;

    // TODO split into passages
    if (FLAG_SNIPPETS_PRECOMPUTE) {
        Passage_Segements res;
        typedef std::vector<Passage_Segement> Passage_Segements;
        
        SentenceBreakIterator breakiterator(document);

        // iterate on a string
        int i = 0;
        while ( breakiterator.next() > 0 ) {
            int start_offset = breakiterator.getStartOffset();
            int end_offset = breakiterator.getEndOffset();
            res.push_back(Passage_Segement(start_offset, end_offset-start_offset+1));
        }

        
        passages_store_[id] = res; 
    }
}

void NativeDocStore::Remove(int id) {
    store_.erase(id);
}

bool NativeDocStore::Has(int id) {
    return store_.count(id) == 1;
}

std::string & NativeDocStore::Get(int id) {
    return store_[id];
}

// for precompute
Passage_Segements & NativeDocStore::GetPassages(int id) {
    return passages_store_[id];
}

void NativeDocStore::Clear() {
    store_.clear();
}

int NativeDocStore::Size() {
    return store_.size();
}


