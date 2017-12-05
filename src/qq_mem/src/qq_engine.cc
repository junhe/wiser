#include "qq_engine.h"
#include "engine_services.h"
#include "utils.h"

void QQSearchEngine::AddDocument(const std::string &title, const std::string &url, 
        const std::string &body, const std::string &offsets) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);
    
    std::cout << "Get in Add document!!!!!\n";
    // Tokenize the document(already pre-processed using scripts)
    // get terms
    std::vector<std::string> terms = utils::explode(body, ' ');
    std::cout << "Got terms: " << terms.size() << std::endl;
    // TODO: get offsets
    std::vector<Offsets> offset_parsed = utils::parse_offsets(offsets);

    // TODO construct term with offset objects

    inverted_index_.AddDocument(doc_id, terms);
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


