#include "qq_engine.h"

#include "utils.h"

// The parameters here will certainly change. Do not rely too much on this.
void QQSearchEngine::AddDocument(const std::string &title, const std::string &url, 
        const std::string &body) {
    int doc_id = NextDocId();
    doc_store_.Add(doc_id, body);

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

// Return the number of document indexed
int QQSearchEngine::LoadLocalDocuments(const std::string &line_doc_path, int n_rows) {
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
InvertedIndex::const_iterator QQSearchEngine::Find(const Term &term) {
  auto it = inverted_index_.Find(term);
  return it;
}



