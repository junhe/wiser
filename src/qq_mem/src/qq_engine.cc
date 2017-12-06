#include "qq_engine.h"
#include "engine_services.h"
#include "utils.h"
#include <assert.h>

void QQSearchEngine::AddDocument(const std::string &title, const std::string &url, 
        const std::string &body, const std::string &offsets) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);
    
    // Tokenize the document(already pre-processed using scripts)
    // get terms
    std::vector<std::string> terms = utils::explode(body, ' ');
    // get offsets
    std::vector<Offsets> offsets_parsed = utils::parse_offsets(offsets);
    // construct term with offset objects
    assert(terms.size() == offsets_parsed.size());
    TermWithOffsetList terms_with_offset = {};
    for (int i = 0; i < terms.size(); i++) {
        TermWithOffset cur_term(terms[i], offsets_parsed[i]);
        terms_with_offset.push_back(cur_term);
    }
    // add document
    std::cout << terms_with_offset.size() << std::endl;
    inverted_index_.AddDocument(doc_id, terms_with_offset);
}

// The parameters here will certainly change. Do not rely too much on this.
void QQSearchEngine::AddDocument(const std::string &title, const std::string &url, 
        const std::string &body) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);

    // Tokenize the document(already pre-processed using scripts)
    std::vector<std::string> terms = utils::explode(body, ' ');
    
    inverted_index_.AddDocument(doc_id, terms);
}

std::string QQSearchEngine::GetDocument(const int &doc_id) {
    return doc_store_.Get(doc_id);
}

int QQSearchEngine::NextDocId() {
    return next_doc_id_++;
}


std::vector<int> QQSearchEngine::Search(const TermList &terms, const SearchOperator &op) {
    return inverted_index_.Search(terms, op); 
}


