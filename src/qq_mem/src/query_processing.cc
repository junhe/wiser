#include "query_processing.h"

namespace qq_search {

std::vector<ResultDocEntry<PostingListDeltaIterator>> ProcessQueryDelta(
     const Bm25Similarity &similarity,
     std::vector<PostingListDeltaIterator> *pl_iterators, 
     const DocLengthStore &doc_lengths,
     const int n_total_docs_in_index,
     const int k,
     const bool is_phase) {
  if (pl_iterators->size() == 1) {
    SingleTermQueryProcessor<PostingListDeltaIterator> qp(similarity, pl_iterators, doc_lengths, 
        n_total_docs_in_index, k);
    return qp.Process();
  } else if (pl_iterators->size() == 2 && is_phase == false) {
    TwoTermNonPhraseQueryProcessor<PostingListDeltaIterator> qp(similarity, pl_iterators, doc_lengths, 
        n_total_docs_in_index, k);
    return qp.Process();
  } else {
    QueryProcessor qp(similarity, pl_iterators, doc_lengths, 
        n_total_docs_in_index, k, is_phase);
    return qp.Process();
  }
}

} // namespace qq_search



