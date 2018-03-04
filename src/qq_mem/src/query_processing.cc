#include "query_processing.h"

namespace qq_search {
  std::vector<ResultDocEntry> ProcessQuery(IteratorPointers *pl_iterators, 
                                           const DocLengthStore &doc_lengths,
                                           const int n_total_docs_in_index,
                                           const int k,
                                           const bool is_phase) {
    if (pl_iterators->size() == 1) {
      SingleTermQueryProcessor qp(pl_iterators, doc_lengths, 
          n_total_docs_in_index, k);
      return qp.Process();
    } else {
      QueryProcessor qp(pl_iterators, doc_lengths, 
          n_total_docs_in_index, k, is_phase);
      return qp.Process();
    }
  }
}






