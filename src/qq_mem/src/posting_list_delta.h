#ifndef POSTING_LIST_DELTA_H
#define POSTING_LIST_DELTA_H

#include <glog/logging.h>

#include "compression.h"


class OffsetPairsIterator {
 public:
  OffsetPairsIterator(const VarintBuffer *data, int byte_offset, 
      const int end_offset)
    :data_(data), byte_offset_(byte_offset), end_offset_(end_offset),
     pair_index_(0) {}

  bool Advance(OffsetPair *pair) {
    if (byte_offset_ == end_offset_) {
      return false;
    }
    
    uint32_t n;
    int len;

    const std::string *data = data_->DataPointer();

    len = utils::varint_decode(*data, byte_offset_, &n);
    byte_offset_ += len;
    std::get<0>(*pair) = n;

    len = utils::varint_decode(*data, byte_offset_, &n);
    byte_offset_ += len;
    std::get<1>(*pair) = n;

    return true;
  }

 private:
  const VarintBuffer *data_;
  int byte_offset_; 
  const int end_offset_; // the start offset of the next posting
  int pair_index_;
};


class PostingListDeltaIterator {
 public:
  PostingListDeltaIterator(const VarintBuffer *data, 
                           const std::vector<DocIdType> *skip_index,
                           const int total_postings,
                           DocIdType prev_doc_id, 
                           int byte_offset)
    :data_(data), 
     skip_index_(skip_index),
     total_postings_(total_postings),
     cur_posting_index_(0),
     prev_doc_id_(prev_doc_id), 
     byte_offset_(byte_offset)
  {
    DecodeToCache();
  }

  bool Advance() {
    if (IsEnd()) {
      return false;
    }

    byte_offset_ = cache_.next_posting_byte_offset_; 
    cur_posting_index_++;

    DecodeToCache();
    return true;
  }

  bool IsEnd() {
    return cur_posting_index_ == total_postings_;
  }

  DocIdType DocId() {
    return cache_.cur_doc_id_;
  }

  int TermFreq() {
    return cache_.cur_term_freq_;
  }

  int OffsetPairStart() {
    return cache_.cur_offset_pairs_start_;
  }

  OffsetPairsIterator OffsetPairsBegin() {
    if (IsEnd()) {
      LOG(FATAL) 
        << "Trying to get offset iterator from a empty posting iterator\n";
    }
    return OffsetPairsIterator(data_, 
                               cache_.cur_offset_pairs_start_,
                               cache_.next_posting_byte_offset_);
  }

  int CurContentBytes() {
    return cache_.cur_content_bytes_;
  }
  
 private:
  void DecodeToCache() {
    int offset = byte_offset_;
    uint32_t delta;
    int len;

    const std::string *data = data_->DataPointer();

    len = utils::varint_decode(*data, offset, &cache_.cur_content_bytes_);
    offset += len;
    cache_.next_posting_byte_offset_ = offset + cache_.cur_content_bytes_;

    len = utils::varint_decode(*data, offset, &delta);
    offset += len;
    len = utils::varint_decode(*data, offset, &cache_.cur_term_freq_);
    offset += len;

    cache_.cur_doc_id_ = prev_doc_id_ + delta;
    cache_.cur_offset_pairs_start_ = offset; 
  }

  const VarintBuffer * data_;
  const std::vector<DocIdType> *skip_index_;
  int byte_offset_; // start byte of the posting
  int cur_posting_index_;
  DocIdType prev_doc_id_;
  const int total_postings_;

  // cache
  struct PostingCache {
    int next_posting_byte_offset_;
    DocIdType cur_doc_id_;
    uint32_t cur_term_freq_;
    int cur_offset_pairs_start_;
    uint32_t cur_content_bytes_;
  };
  PostingCache cache_;
};


class PostingListDelta {
 public:
  PostingListDelta(Term term) :term_(term), skip_span_(100) {}
  PostingListDelta(Term term, const int skip_span) 
    :term_(term), skip_span_(skip_span) {}

  // Assume posting[-1] = posting[0], so delta[0] is always 0
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
    return PostingListDeltaIterator(&data_, &skip_index_, Size(), 
        skip_index_[0], 0);
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
  // [0]: doc id of posting[-1]
  // [1]: doc id of posting[skip_span_ - 1] 
  // [2]: doc id of posting[skip_span_ * 2 - 1] 
  // ...
  std::vector<DocIdType> skip_index_;
  const int skip_span_;
};

#endif
