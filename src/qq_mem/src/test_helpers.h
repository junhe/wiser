#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include "posting.h"
#include "posting_list_delta.h"
#include "posting_list_vec.h"
#include "flash_iterators.h"

StandardPosting create_posting(DocIdType doc_id, 
                               int term_freq, 
                               int n_offset_pairs);
StandardPosting create_posting(DocIdType doc_id, 
                               int term_freq, 
                               int n_offset_pairs,
                               int n_postings);
PostingList_Vec<StandardPosting> create_posting_list_vec(int n_postings);
PostingListStandardVec create_posting_list_standard(int n_postings);
PostingListDelta create_posting_list_delta(int n_postings);


// Dump a cozy box, return FileOffsetsOfBlobs
FileOffsetsOfBlobs DumpCozyBox(std::vector<uint32_t> vec, 
    const std::string path, bool do_delta);

SkipList CreateSkipList(const std::string type, std::vector<off_t> offsets_of_bags);
SkipList CreateSkipListForDodId(
    std::vector<uint32_t> skip_doc_ids, std::vector<off_t> offsets_of_bags);
SkipList CreateSkipListForPosition( std::vector<off_t> blob_offsets, 
                                    std::vector<int> in_blob_indexes);
SkipList CreateSkipListForOffset( std::vector<off_t> blob_offsets, 
                                    std::vector<int> in_blob_indexes);
#endif
