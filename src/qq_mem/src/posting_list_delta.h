#ifndef POSTING_LIST_DELTA_H
#define POSTING_LIST_DELTA_H

#include <glog/logging.h>

#include "compression.h"
#include "engine_services.h"


class CompressedPositionIterator: public PopIteratorService {
 public:
  // The data should not contain the size of the position segment
  CompressedPositionIterator(const std::string *data, 
      const int start_offset, const int end_offset)
    :iterator_(data, start_offset, end_offset) {}

  bool IsEnd() const {
    return iterator_.IsEnd();
  }

  uint32_t Pop() {
    uint32_t pos = last_pos_ + iterator_.Pop();
    last_pos_ = pos;
    return pos;
  }

 private:
  VarintIteratorEndBound iterator_;
  uint32_t last_pos_ = 0;
};


class CompressedPairIterator: public OffsetPairsIteratorService {
 public:
  CompressedPairIterator(const std::string *data, const int start_offset, 
      const int end_offset)
    :iterator_(data, start_offset, end_offset) {}

  bool IsEnd() const {
    return iterator_.IsEnd();
  }

  // Make sure you are not at the end by IsEnd() before calling
  // this function.
  void Pop(OffsetPair *pair) {
    uint32_t offset;

    offset = last_offset_ + iterator_.Pop();
    std::get<0>(*pair) = offset;
    last_offset_ = offset;

    offset = last_offset_ + iterator_.Pop();
    std::get<1>(*pair) = offset;
    last_offset_ = offset;
  }

 private:
  VarintIteratorEndBound iterator_;
  uint32_t last_offset_ = 0;
};


struct SpanMeta {
  DocIdType prev_doc_id; // doc ID right before this span
  int start_offset;

  SpanMeta(DocIdType id, int offset)
    : prev_doc_id(id), start_offset(offset) {}

  SpanMeta(const std::string &data, const int offset) {
    Deserialize(data, offset);
  }

  friend bool operator == (const SpanMeta &a, const SpanMeta &b) {
    return a.prev_doc_id == b.prev_doc_id && a.start_offset == b.start_offset;
  }

  friend bool operator != (const SpanMeta &a, const SpanMeta &b) {
    return !(a == b);
  }

  std::string Serialize() const {
    VarintBuffer buf;
    buf.Append(prev_doc_id);
    buf.Append(start_offset);

    return buf.Data();
  }

  void Deserialize(const std::string &data, const off_t offset) {
    VarintIterator it(&data, offset, 2); 
    prev_doc_id = it.Pop();
    start_offset = it.Pop();
  }
};


struct SkipIndex {
  std::vector<SpanMeta> vec;

  // count | meta size | meta | meta size | meta ..
  std::string Serialize() const {
    VarintBuffer buf;

    buf.Append(vec.size());
    
    for (auto &meta : vec) {
      std::string meta_data = meta.Serialize();
      buf.Append(meta_data.size());
      buf.Append(meta_data);
    }

    return buf.Data();
  }

  friend bool operator == (const SkipIndex &a, const SkipIndex &b) {
    if (a.vec.size() != b.vec.size()) 
      return false;

    for (int i = 0; i < a.vec.size(); i++) {
      if (a.vec[i] != b.vec[i]) 
        return false;
    }

    return true;
  }

  friend bool operator != (const SkipIndex &a, const SkipIndex &b) {
    return !(a == b);
  }

  int Deserialize(const std::string &data, off_t start_offset) {
    int len;  
    uint32_t size; 
    uint32_t meta_size;
    off_t offset = start_offset;

    vec.clear();

    len = utils::varint_decode(data, offset, &size);
    offset += len;

    for (int i = 0; i < size; i++) {
      len = utils::varint_decode(data, offset, &meta_size);
      offset += len;

      vec.emplace_back(data, offset);
      offset += meta_size;
    }

    return offset - start_offset;
  }
};


class PostingListDeltaIterator: public PostingListIteratorService {
 public:
  PostingListDeltaIterator(const VarintBuffer *data, 
                           const SkipIndex *skip_index,
                           const int skip_span,
                           const int total_postings,
                           DocIdType prev_doc_id, 
                           int byte_offset)
    :data_(data), 
     skip_index_(skip_index),
     skip_span_(skip_span),
     total_postings_(total_postings),
     cur_state_(*data->DataPointer(), byte_offset, 0, prev_doc_id)
  {
    DecodeToCache();
  }

  int Size() const {
    return total_postings_;
  }

  bool Advance() {
    if (IsEnd()) {
      return false;
    }

    cur_state_.SetPosting(cache_.next_posting_byte_offset_, 
                  cur_state_.cur_posting_index_ + 1,
                  cache_.cur_doc_id_);
                  
    DecodeToCache();
    return true;
  }

  bool HasSkip() const {
    return cur_state_.cur_posting_index_ % skip_span_ == 0 && 
      cur_state_.cur_posting_index_ + skip_span_ < total_postings_;
  }

  // Only call this when the iterator HasSkip() == true
  DocIdType NextSpanDocId() const {
    int index = (cur_state_.cur_posting_index_ / skip_span_) + 1;
    return skip_index_->vec[index].prev_doc_id;
  }
  
  // Only call this when the iterator HasSkip() == true
  void SkipToNextSpan() {
    int next_span_index = cur_state_.cur_posting_index_ / skip_span_ + 1;

    auto &meta = skip_index_->vec[next_span_index];
    cur_state_.SetPosting(meta.start_offset, next_span_index * skip_span_, 
                  meta.prev_doc_id);

    DecodeToCache();
  }

  void SkipForward(DocIdType doc_id) {
    // Move the iterator to a posting that has a doc id that is 
    // larger or equal to doc_id
    // It moves to the end if it cannout find such posting
    // TODO: Advance() with less cost

    // loop inv: 
    //   posting[0, cur_posting_index_).doc_id < doc_id
    //   posting_start_offset_ is the offset of posting[cur_posting_index_]
    //   prev_doc_id_ is the doc id of posting[cur_posting_index_ - 1]
    //   cache_ has the data of posting[cur_posting_index_]

    while (cur_state_.cur_posting_index_ < total_postings_ && cache_.cur_doc_id_ < doc_id) {
      if (HasSkip() && NextSpanDocId() < doc_id) {
        SkipToNextSpanWithoutDecoding();
      } else {
        AdvanceWithoutDecoding();
      }
      DecodeUntil(1);
    }
  }

  bool IsEnd() const {
    return cur_state_.cur_posting_index_ == total_postings_;
  }

  DocIdType DocId() const {
    return cache_.cur_doc_id_;
  }

  int TermFreq() const {
    return cache_.cur_term_freq_;
  }

  std::unique_ptr<OffsetPairsIteratorService> OffsetPairsBegin() const {
    if (IsEnd()) {
      LOG(FATAL) 
        << "Trying to get offset iterator from a empty posting iterator\n";
    }
    std::unique_ptr<OffsetPairsIteratorService> p(new
        CompressedPairIterator(data_->DataPointer(), 
                               cache_.cur_offset_pairs_start_,
                               cache_.cur_position_start_)); 
    return p; 
  }

  std::unique_ptr<PopIteratorService> PositionBegin() const {
    std::unique_ptr<PopIteratorService> p(new CompressedPositionIterator(
          data_->DataPointer(), 
          cache_.cur_position_start_, 
          cache_.next_posting_byte_offset_));
    return p;
  }

 private:
  void AdvanceWithoutDecoding() {
    cur_state_.SetPosting(cache_.next_posting_byte_offset_, 
                  cur_state_.cur_posting_index_ + 1,
                  cache_.cur_doc_id_);
  }

  // Only call this when the iterator HasSkip() == true
  void SkipToNextSpanWithoutDecoding() {
    int next_span_index = cur_state_.cur_posting_index_ / skip_span_ + 1;

    auto &meta = skip_index_->vec[next_span_index];
    cur_state_.SetPosting(meta.start_offset, next_span_index * skip_span_, 
                  meta.prev_doc_id);
  }

  // After this function, item_index will be decoded, item_index + 1 will not.
  void DecodeUntil(int item_index) {
    for( int i = cur_state_.next_item_to_decode_;
         i <= item_index; 
         i++ ) {
      if (i == 0) {
        cur_state_.PopInPosting(&cache_.cur_content_bytes_);
        cache_.next_posting_byte_offset_ = cur_state_.Offset() + cache_.cur_content_bytes_;
      } else if (i == 1) {
        uint32_t delta;
        cur_state_.PopInPosting(&delta);
        cache_.cur_doc_id_ = cur_state_.prev_doc_id_ + delta;
      } else if (i == 2) {
        cur_state_.PopInPosting(&cache_.cur_term_freq_);
      } else if (i == 3) {
        cur_state_.PopInPosting(&cache_.offset_size_);
        cache_.cur_offset_pairs_start_ = cur_state_.Offset(); 
        cache_.cur_position_start_ = cur_state_.Offset() + cache_.offset_size_;
      }
    }
  }

  void DecodeToCache() {
    DecodeUntil(3);
  }

  const VarintBuffer * data_;
  const SkipIndex *skip_index_;
  const int total_postings_;
  const int skip_span_;

  // Only set member variables through setters.
  // You can read them directly.
  // This is to avoid extra function calls.
  struct State {
    const std::string &data_;
    off_t posting_start_offset_; // start byte of posting[cur_posting_index_]
    int in_posting_offset_; // offset of the next item to decode
    int cur_posting_index_;
    int next_item_to_decode_;
    DocIdType prev_doc_id_; // doc id of posting[cur_posting_index_ - 1]

    State(const std::string &data, off_t offset, int index, DocIdType id)
      :data_(data),
       posting_start_offset_(offset), 
       in_posting_offset_(0),
       next_item_to_decode_(0),
       cur_posting_index_(index), 
       prev_doc_id_(id) {}

    off_t Offset() {
      return posting_start_offset_ + in_posting_offset_;
    }

    void SetPosting(off_t offset, int index, DocIdType id) {
      posting_start_offset_ = offset;
      in_posting_offset_ = 0;
      next_item_to_decode_ = 0;
      cur_posting_index_ = index;
      prev_doc_id_ = id;
    }

    // return the index of item just popped.
    int PopInPosting(uint32_t *p) {
      int len;
      int ret = next_item_to_decode_;
      len = utils::varint_decode(data_, posting_start_offset_ + in_posting_offset_, p);
      in_posting_offset_ += len;
      next_item_to_decode_++;
      return ret;
    }
  };
  State cur_state_;

  struct PostingCache {             // available when next_item_to_decode_ > x
    uint32_t cur_content_bytes_;    // 0
    int next_posting_byte_offset_;  // 0

    DocIdType cur_doc_id_;          // 1
    uint32_t cur_term_freq_;        // 2
    uint32_t offset_size_;          // 3
    int cur_offset_pairs_start_;    // 3
    int cur_position_start_;        // 3
  };
  // Cached data of cur_posting_index_
  PostingCache cache_;
};


class PostingListDelta {
 public:
  PostingListDelta(Term term) :skip_span_(100) {}
  PostingListDelta(Term term, const int skip_span) 
    :skip_span_(skip_span) {}

  // Assume posting[-1] = posting[0], so delta[0] is always 0
  void AddPosting(const StandardPosting &posting) {
    DocIdType doc_id = posting.GetDocId();

    if (posting_idx_ == 0) {
      // initialize for the first posting
      last_doc_id_ = doc_id;
    }

    if ( posting_idx_ > 0 && doc_id <= last_doc_id_) {
      LOG(FATAL) << "Doc id should not be less than the previous one. "
        << "prev: " << last_doc_id_ << " this: " << doc_id;
    }

    if (posting_idx_ % skip_span_ == 0) {
      skip_index_.vec.push_back(SpanMeta(last_doc_id_, data_.End()));
    }

    DocIdType delta = doc_id - last_doc_id_;
    StandardPosting posting_with_delta(delta, 
                                                 posting.GetTermFreq(), 
                                                 *posting.GetOffsetPairs(),
                                                 *posting.GetPositions()
                                                 );
    data_.Append(posting_with_delta.Encode());

    last_doc_id_ = doc_id;
    posting_idx_++;
  }

  std::unique_ptr<PostingListIteratorService> Begin() const {
    if (posting_idx_ == 0) {
      LOG(FATAL) << "Posting List must have at least one posting" << std::endl;
    }
    return std::unique_ptr<PostingListIteratorService>(new PostingListDeltaIterator(
          &data_, 
          &skip_index_, 
          skip_span_, 
          Size(), 
          skip_index_.vec[0].prev_doc_id, 
          0));
  }

  std::unique_ptr<PostingListDeltaIterator> NativeBegin() const {
    if (posting_idx_ == 0) {
      LOG(FATAL) << "Posting List must have at least one posting" << std::endl;
    }
    return std::unique_ptr<PostingListDeltaIterator>(new PostingListDeltaIterator(
          &data_, 
          &skip_index_, 
          skip_span_, 
          Size(), 
          skip_index_.vec[0].prev_doc_id, 
          0));
  }

  int Size() const {
    return posting_idx_;
  }

  int ByteCount() const {
    return data_.Size();
  }

  SkipIndex GetSkipIndex() const {
    return skip_index_;
  }

  friend bool operator == (const PostingListDelta &a, const PostingListDelta &b) {
    if (a.last_doc_id_ != b.last_doc_id_) {
      return false;
    }

    if (a.posting_idx_ != b.posting_idx_) {
      return false;
    }

    if (a.skip_span_ != b.skip_span_) {
      return false;
    }

    if (a.data_.Data() != b.data_.Data()) {
      return false;
    }

    if (a.skip_index_ != b.skip_index_) {
      return false;
    }

    return true;
  }

  friend bool operator != (const PostingListDelta &a, const PostingListDelta &b) {
    return !(a == b);
  }

  std::string Serialize() const {
    VarintBuffer buf;
    buf.Append(last_doc_id_);
    buf.Append(posting_idx_);
    buf.Append(skip_span_);
    

    std::string data = data_.Serialize();
    buf.Append(data.size());
    buf.Append(data);

    std::string index_data = skip_index_.Serialize();
    buf.Append(index_data.size());
    buf.Append(index_data);

    return buf.Data();
  }

  void Deserialize(const std::string &data, const off_t start_offset) {
    int len;
    off_t offset = start_offset;
    uint32_t var;

    len = utils::varint_decode(data, offset, &var);
    offset += len;
    last_doc_id_ = var;

    len = utils::varint_decode(data, offset, &var);
    offset += len;
    posting_idx_ = var;

    len = utils::varint_decode(data, offset, &var);
    offset += len;
    skip_span_ = var;

    len = utils::varint_decode(data, offset, &var);
    offset += len;
    int data_size  = var;

    data_.Deserialize(data, offset);
    offset += data_size;

    len = utils::varint_decode(data, offset, &var);
    offset += len;
    int skip_index_size  = var;

    skip_index_.Deserialize(data, offset);
    offset += skip_index_size;
  }

 private:
  DocIdType last_doc_id_;
  int posting_idx_ = 0; 
  int skip_span_;
  VarintBuffer data_;
  // [0]: doc id of posting[-1]
  // [1]: doc id of posting[skip_span_ - 1] 
  // [2]: doc id of posting[skip_span_ * 2 - 1] 
  // ...
  SkipIndex skip_index_;

};

#endif
