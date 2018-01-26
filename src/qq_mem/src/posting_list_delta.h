#ifndef POSTING_LIST_DELTA_H
#define POSTING_LIST_DELTA_H

#include "compression.h"

class PostingListDelta {
 public:
  PostingListDelta(Term term) :term_(term){}

  void AddPosting(const RankingPostingWithOffsets &posting) {
    data_.Append(posting.Encode());
    n_postings_++;
  }

  int Size() {
    return n_postings_;
  }

  int ByteCount() {
    return data_.Size();
  }

 private:
  VarintBuffer data_;
  const Term term_;
  int n_postings_ = 0;
};

#endif
