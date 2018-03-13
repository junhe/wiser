#include "query_processing.h"

namespace qq_search {

std::vector<ResultDocEntry2> ProcessQueryDelta(
     const Bm25Similarity &similarity,
     std::vector<PostingListDeltaIterator> *pl_iterators, 
     const DocLengthStore &doc_lengths,
     const int n_total_docs_in_index,
     const int k,
     const bool is_phase) {
  if (pl_iterators->size() == 1) {
    SingleTermQueryProcessor2 qp(similarity, pl_iterators, doc_lengths, 
        n_total_docs_in_index, k);
    return qp.Process();
  } else if (pl_iterators->size() == 2 && is_phase == false) {
    TwoTermNonPhraseQueryProcessor2 qp(similarity, pl_iterators, doc_lengths, 
        n_total_docs_in_index, k);
    return qp.Process();
  } else {
    QueryProcessor2 qp(similarity, pl_iterators, doc_lengths, 
        n_total_docs_in_index, k, is_phase);
    return qp.Process();
  }
}

} // namespace qq_search



