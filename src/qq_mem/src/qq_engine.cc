#include "qq_engine.h"
#include "utils.h"

// The parameters here will certainly change. Do not rely too much on this.
void QQEngine::AddDocument(const std::string &title, const std::string &url, 
        const std::string &body) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);

    std::vector<std::string> terms = utils::explode(body, ' ');
    inverted_index_.AddDocument(doc_id, terms);
}

int QQEngine::NextDocId() {
    return next_doc_id_++;
}


std::vector<int> QQEngine::Search(const TermList &terms, const SearchOperator &op) {
    return inverted_index_.Search(terms, op); 
}


