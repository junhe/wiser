#include "posting_list_vec.h"

#include <iostream>
#include <glog/logging.h>


PostingList_Vec::PostingList_Vec(const std::string &term)
  :term_(term) {
}

// You must make sure postings are added with increasing doc ID
void PostingList_Vec::AddPosting(const Posting &posting) {
  // TODO: check doc id increasing
  if (posting_store_.back().GetDocID() < posting.GetDocID()) {
    LOG(FATAL) 
      << "New posting doc ID must be larger than the last existing one."
      << std::endl;
  }
  posting_store_.push_back(posting);
}

