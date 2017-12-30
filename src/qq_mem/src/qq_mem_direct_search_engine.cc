#include "qq_mem_direct_search_engine.h"
#include "types.h"
#include "utils.h" 
#include <assert.h>

void QQMemDirectSearchEngine::AddDocument(const std::string &title, const std::string &url, 
                                 const std::string &body, const std::string &tokens,
                                 const std::string &offsets) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);
    
    // Tokenize the document(already pre-processed using scripts)
    std::vector<std::string> terms = utils::explode(tokens, ' '); // Get all terms occured in this doc
    std::vector<Offsets> offsets_parsed = utils::parse_offsets(offsets); // Get all offsets for each term  occured in this doc
    
    // Construct term with offset objects
    assert(terms.size() == offsets_parsed.size());
    TermWithOffsetList terms_with_offset = {};
    for (int i = 0; i < terms.size(); i++) {
        Offsets tmp_offsets = offsets_parsed[i];
        TermWithOffset cur_term(terms[i], tmp_offsets);
        terms_with_offset.push_back(cur_term);
    }
    // Add document with terms and offsets information
    inverted_index_.AddDocument(doc_id, terms_with_offset);
}

void QQMemDirectSearchEngine::AddDocument(const std::string &title, const std::string &url, 
                                 const std::string &body) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);
    // Tokenize the document(already pre-processed using scripts)
    std::vector<std::string> terms = utils::explode(body, ' ');
    
    inverted_index_.AddDocument(doc_id, terms);
}

const std::string & QQMemDirectSearchEngine::GetDocument(const int &doc_id) {
    return doc_store_.Get(doc_id);
}

std::vector<int> QQMemDirectSearchEngine::Search(const TermList &terms, const SearchOperator &op) {
    return inverted_index_.Search(terms, op); 
}

// Return the number of document indexed
int QQMemDirectSearchEngine::LoadLocalDocuments(const std::string &line_doc_path, int n_rows) {
  utils::LineDoc linedoc(line_doc_path);
  std::vector<std::string> items;
  bool has_it;

  int count = 0;
  for (int i = 0; i < n_rows; i++) {
    has_it = linedoc.GetRow(items);
    if (has_it) {
      AddDocument(items[0], "http://wiki", items[2]);
      count++;
    } else {
      break;
    }

    if (count % 10000 == 0) {
      std::cout << "Indexed " << count << " documents" << std::endl;
    }
  }

  return count;
}

// This function is currently for benchmarking
// It returns an iterator without parsing the posting list
InvertedIndex::const_iterator QQMemDirectSearchEngine::Find(const Term &term) {
  auto it = inverted_index_.Find(term);
  return it;
}

