#include "qq_engine.h"

// The parameters here will certainly change. Do not rely too much on this.
void QQEngine::AddDocument(const std::string &title, const std::string &url, 
        const std::string &body) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);


    // inverted_index_.AddDocument(doc_id, TermList{"hello", "world"});
}

int QQEngine::NextDocId() {
    return next_doc_id_++;
}

