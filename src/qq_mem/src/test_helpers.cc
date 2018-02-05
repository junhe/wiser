#include "test_helpers.h"

StandardPosting create_posting(DocIdType doc_id, 
                                         int term_freq,
                                         int n_offset_pairs) {
  return create_posting(doc_id, term_freq, n_offset_pairs, 0);
}


StandardPosting create_posting(DocIdType doc_id, 
                                         int term_freq,
                                         int n_offset_pairs,
                                         int n_positions) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < n_offset_pairs; i++) {
    offset_pairs.push_back(std::make_tuple(i * 2, i * 2 + 1)); 
  }

  Positions positions;
  for (int i = 0; i < n_positions; i++) {
    positions.push_back(i);
  }

  StandardPosting posting(doc_id, term_freq, offset_pairs, positions); 
  return posting;
}


PostingList_Vec<StandardPosting> create_posting_list_vec(int n_postings) {
  PostingList_Vec<StandardPosting> pl("hello");
  for (int i = 0; i < n_postings; i++) {
    pl.AddPosting(create_posting(i, i * 2, 5));
  }
  return pl;
}

PostingListStandardVec create_posting_list_standard(int n_postings) {
  PostingListStandardVec pl("hello");
  for (int i = 0; i < n_postings; i++) {
    pl.AddPosting(create_posting(i, i * 2, 5, 6));
  }
  return pl;
}


