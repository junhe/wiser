#include "query_processing.h"


qq_float aggregate_term_score(const TermScoreMap &term_scores) {
  qq_float doc_score = 0;
  for (TermScoreMap::const_iterator it = term_scores.cbegin();
       it != term_scores.cend(); it++) 
  {
    doc_score += it->second; 
  }
  return doc_score;
}


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






