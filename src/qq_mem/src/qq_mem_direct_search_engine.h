#ifndef QQ_MEM_DIRECT_SEARCH_ENGINE_H
#define QQ_MEM_DIRECT_SEARCH_ENGINE_H

#include <string>
#include <iostream>

#include "engine_services.h"
#include "inverted_index.h"
#include "native_doc_store.h"


// This class is one of the search engine implementations
// in this implementation:
//   1. all contents are stored in mem
//   2. all contents are directly stored (no serialization) 
class QQMemDirectSearchEngine: public SearchEngineService {
 public:
  // Add one more document into this search engine document base
  void AddDocument(const std::string &title, const std::string &url, 
                   const std::string &body);
  void AddDocument(const std::string &title, const std::string &url, 
                   const std::string &body, const std::string &tokens,
                   const std::string &offsets); // with analyzed info(tokens, offsets) about this doc
  // Get document content according to unique doc_id
  const std::string & GetDocument(const int &doc_id);
  // Search a query, get top-related document ids 
  std::vector<int> Search(const TermList &terms, const SearchOperator &op);
  // Batch-load local documents into this search engine document base
  int LoadLocalDocuments(const std::string &line_doc_path, int n_rows);
  InvertedIndex::const_iterator Find(const Term &term);
 private:
  NativeDocStore doc_store_;
  InvertedIndex inverted_index_;
};

#endif
