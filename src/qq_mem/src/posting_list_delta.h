#ifndef POSTING_LIST_DELTA_H
#define POSTING_LIST_DELTA_H

#include <glog/logging.h>

#include "compression.h"


class PostingDeltaReader {
 public:
  PostingDeltaReader(const std::string *posting_list, const int offset)
    :posting_list_(posting_list), offset_(offset) 
  {}

  DocIdType GetDocId() const {

  }

  int GetTermFreq() const {

  }

  // GetPosting(some iterator)
  // GetOffsetPairs()

  int GetByteCount() const {
  }

 private:
  const std::string *posting_list_;
  const int offset_;
};


class PostingListDeltaIterator {
 public:
  PostingListDeltaIterator(const VarintBuffer *data, 
                           const std::vector<DocIdType> *skip_index,
                           DocIdType cur_doc_id, 
                           int posting_index, 
                           int byte_offset)
    :data_(data), 
     skip_index_(skip_index),
     cur_doc_id_(cur_doc_id), 
     posting_index_(posting_index),
     byte_offset_(byte_offset)
  {
    
  
  }

 private:
  const VarintBuffer * data_;
  const std::vector<DocIdType> *skip_index_;
  DocIdType cur_doc_id_;
  int posting_index_;
  int byte_offset_; // start byte of the posting
};


class PostingListDelta {
 public:
  PostingListDelta(Term term) :term_(term), skip_span_(100) {}
  PostingListDelta(Term term, const int skip_span) 
    :term_(term), skip_span_(skip_span) {}

  void AddPosting(const RankingPostingWithOffsets &posting) {
    DocIdType doc_id = posting.GetDocId();

    if (posting_idx_ == 0) {
      // initialize for the first posting
      last_doc_id_ = doc_id;
    }

    if (posting_idx_ % skip_span_) {
      skip_index_.push_back(last_doc_id_);
    }

    DocIdType delta = doc_id - last_doc_id_;
    RankingPostingWithOffsets posting_with_delta(delta, 
                                                 posting.GetTermFreq(), 
                                                 *posting.GetOffsetPairs());
    data_.Append(posting_with_delta.Encode());

    last_doc_id_ = doc_id;
    posting_idx_++;
  }

  PostingListDeltaIterator Begin() {
    if (posting_idx_ == 0) {
      LOG(FATAL) << "Posting List must have at least one posting" << std::endl;
    }
    return PostingListDeltaIterator(&data_, &skip_index_, skip_index_[0], 0, 0);
  }

  int Size() {
    return posting_idx_;
  }

  int ByteCount() {
    return data_.Size();
  }

 private:
  VarintBuffer data_;
  const Term term_;
  int posting_idx_ = 0; 
  DocIdType last_doc_id_;
  // [0]: doc if of first posting
  // [1]: doc if of posting[skip_span_] 
  // [2]: doc if of posting[skip_span_*2] 
  // ...
  std::vector<DocIdType> skip_index_;
  const int skip_span_;
};

#endif
