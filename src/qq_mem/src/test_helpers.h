#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include "posting.h"
#include "posting_list_vec.h"

StandardPosting create_posting(DocIdType doc_id, 
                               int term_freq, 
                               int n_offset_pairs);
StandardPosting create_posting(DocIdType doc_id, 
                               int term_freq, 
                               int n_offset_pairs,
                               int n_postings);
PostingList_Vec<StandardPosting> create_posting_list_vec(int n_postings);
PostingListStandardVec create_posting_list_standard(int n_postings);

#endif
