#include "qq_engine.h"
#include "engine_services.h"
#include "unifiedhighlighter.h" // TODO change the API
#include "utils.h"
#include <assert.h>

// For precomputation, insert splits into offsets(with fake offset)
void QQSearchEngine::precompute_insert_splits(Passage_Segements & splits, Offsets & offsets_in) {
    // insert split (after split, there are offset ranges for each split)
    if (offsets_in.size() > 0)
        offsets_in.push_back(Offset(-1, -1));

    // calculate scores, add to final
    int start_one; 
    int end_one = -1;
    for (int i = 0; i < splits.size(); i++) {
        // calculate range for splits[i]
        start_one = end_one + 1;
        int start_offset = splits[i].first;
        int end_offset = splits[i].second + start_offset - 1;
        while(std::get<0>(offsets_in[start_one]) < start_offset)
            start_one ++;
        end_one = start_one;
        while (std::get<1>(offsets_in[end_one]) <= end_offset)
            end_one ++;
        end_one--;
        int len = end_one - start_one + 1;
        if (len > 0) {
             UnifiedHighlighter tmp_highlighter;   // just for scoring 
            // store the score
            int passage_length = splits[i].second;
            int score = (int)(tmp_highlighter.tf_norm(len, passage_length)*1000000000);
            std::cout << "score: " << score << std::endl;
            offsets_in.push_back(Offset(i, score));
        }
    }
    return ;
}

void QQSearchEngine::AddDocument(const std::string &title, const std::string &url, 
        const std::string &body, const std::string &tokens,  const std::string &offsets) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);
    
    // Tokenize the document(already pre-processed using scripts)
    // get terms
    std::vector<std::string> terms = utils::explode(tokens, ' ');
    // get offsets
    std::vector<Offsets> offsets_parsed = utils::parse_offsets(offsets);
    // construct term with offset objects
    assert(terms.size() == offsets_parsed.size());
    TermWithOffsetList terms_with_offset = {};
    for (int i = 0; i < terms.size(); i++) {
        Offsets tmp_offsets = offsets_parsed[i];
        if (FLAG_SNIPPETS_PRECOMPUTE) {
            precompute_insert_splits(doc_store_.GetPassages(doc_id), tmp_offsets);
        }
        TermWithOffset cur_term(terms[i], tmp_offsets);
        terms_with_offset.push_back(cur_term);
    }
    // add document
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

std::string & QQSearchEngine::GetDocument(const int &doc_id) {
    return doc_store_.Get(doc_id);
}

int QQSearchEngine::NextDocId() {
    return next_doc_id_++;
}


std::vector<int> QQSearchEngine::Search(const TermList &terms, const SearchOperator &op) {
    return inverted_index_.Search(terms, op); 
}

Passage_Segements & QQSearchEngine::GetDocumentPassages(const int &doc_id) {
    return doc_store_.GetPassages(doc_id);
}
