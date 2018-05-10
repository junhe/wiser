#ifndef POSTING_LIST_DELTA_H
#define POSTING_LIST_DELTA_H

#include <glog/logging.h>

#include "posting.h"
#include "compression.h"
#include "engine_services.h"


class CompressedPositionIterator: public PopIteratorService {
 public:
  // The data should not contain the size of the position segment
  CompressedPositionIterator(const std::string *data, 
      const int start_offset, const int end_offset)
    :iterator_(data, start_offset, end_offset) {}

  CompressedPositionIterator() {}

  bool IsEnd() const {
    return iterator_.IsEnd();
  }

  uint64_t Pop() {
    uint64_t pos = last_pos_ + iterator_.Pop();
    last_pos_ = pos;
    return pos;
  }

 private:
  VarintIteratorEndBound iterator_;
  uint64_t last_pos_ = 0;
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

    for (std::size_t i = 0; i < a.vec.size(); i++) {
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

    for (uint32_t i = 0; i < size; i++) {
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
  PostingListDeltaIterator() 
    :data_pointer_(nullptr), 
     pl_addr_(nullptr),
     skip_index_(nullptr),
     skip_span_(-1),
     total_postings_(0),
     cur_state_(nullptr, 0, 0)
  {}

  PostingListDeltaIterator(const VarintBuffer *data, 
                           const SkipIndex *skip_index,
                           const int skip_span,
                           const int total_postings,
                           DocIdType prev_doc_id, 
                           int byte_offset)
    :data_pointer_(data->DataPointer()),
     pl_addr_((const uint8_t *)data->DataPointer()->data()),
     skip_index_(skip_index),
     skip_span_(skip_span),
     total_postings_(total_postings),
     cur_state_((const uint8_t *)(data->DataPointer()->data()), 0, prev_doc_id)
  {
    DecodeToCache();
  }

  PostingListDeltaIterator(const PostingListDeltaIterator &rhs) = default;

  PostingListDeltaIterator& operator=(const PostingListDeltaIterator &rhs) = default;

  int Size() const noexcept {
    return total_postings_;
  }

  void Advance() noexcept {
    cur_state_.Update(
                  cache_.next_posting_addr_,
                  cur_state_.cur_posting_index_ + 1,
                  cache_.cur_doc_id_);
                  
    DecodeToCache();
  }

  bool HasSkip() const noexcept {
    return cur_state_.cur_posting_index_ % skip_span_ == 0 && 
      cur_state_.cur_posting_index_ + skip_span_ < total_postings_;
  }

  // Only call this when the iterator HasSkip() == true
  DocIdType NextSpanDocId() const noexcept {
    int index = (cur_state_.cur_posting_index_ / skip_span_) + 1;
    return skip_index_->vec[index].prev_doc_id;
  }
  
  // Only call this when the iterator HasSkip() == true
  void SkipToNextSpan() noexcept {
    int next_span_index = cur_state_.cur_posting_index_ / skip_span_ + 1;

    auto &meta = skip_index_->vec[next_span_index];
    cur_state_.Update(
        (const uint8_t *)(data_pointer_->data()) + meta.start_offset,
        next_span_index * skip_span_, 
        meta.prev_doc_id);

    DecodeToCache();
  }

  void SkipForward(DocIdType doc_id) noexcept {
    // Move the iterator to a posting that has a doc id that is 
    // larger or equal to doc_id
    // It moves to the end if it cannout find such posting
    // TODO: Advance() with less cost

    // loop inv: 
    //   posting[0, cur_posting_index_).doc_id < doc_id
    //   byte_offset_ is the offset of posting[cur_posting_index_]
    //   prev_doc_id_ is the doc id of posting[cur_posting_index_ - 1]
    //   cache_ has the data of posting[cur_posting_index_]

    while (cur_state_.cur_posting_index_ < total_postings_ && cache_.cur_doc_id_ < doc_id) {
      if (HasSkip() && NextSpanDocId() < doc_id) {
        SkipToNextSpan();
      } else {
        Advance();
      }
    }
  }

  bool IsEnd() const noexcept {
    return cur_state_.cur_posting_index_ == total_postings_;
  }

  DocIdType DocId() const noexcept {
    return cache_.cur_doc_id_;
  }

  int TermFreq() const noexcept {
    return cache_.cur_term_freq_;
  }

  std::unique_ptr<OffsetPairsIteratorService> OffsetPairsBegin() const {
    std::unique_ptr<OffsetPairsIteratorService> p(new
        CompressedPairIterator(data_pointer_, 
                  cache_.cur_offset_pairs_start_addr_ - pl_addr_,
                  cache_.cur_position_start_addr_ - pl_addr_));
    return p; 
  }

  std::unique_ptr<PopIteratorService> PositionBegin() const {
    std::unique_ptr<PopIteratorService> p(new CompressedPositionIterator(
          data_pointer_, 
          cache_.cur_position_start_addr_ - pl_addr_,
          cache_.next_posting_addr_ - pl_addr_));
    return p;
  }

  void AssignPositionBegin(CompressedPositionIterator *iterator) const {
    new (iterator) CompressedPositionIterator(  
          data_pointer_, 
          cache_.cur_position_start_addr_ - pl_addr_,
          cache_.next_posting_addr_ - pl_addr_);
  }

 private:
  // cur_state_.cur_addr_ must at the beginning of a posting
  void DecodeToCache() noexcept {
    uint32_t delta;

    // 0. Content bytes
    DecodeVarint(&cache_.cur_content_bytes_);
    cache_.next_posting_addr_ = cur_state_.cur_addr_ + cache_.cur_content_bytes_;

    // 1. Doc Id delta
    DecodeVarint(&delta);
    cache_.cur_doc_id_ = cur_state_.prev_doc_id_ + delta;

    // 2. Term freq
    DecodeVarint(&cache_.cur_term_freq_);

    // 3. offset size
    DecodeVarint(&cache_.offset_size_);
    cache_.cur_offset_pairs_start_addr_ = cur_state_.cur_addr_;
    cache_.cur_position_start_addr_ = cur_state_.cur_addr_ + cache_.offset_size_;
  }

  // Note: it changes cur_state_.cur_addr_
  void DecodeVarint(uint32_t *value) noexcept {
    uint32_t v = *cur_state_.cur_addr_;
    if (v < 0x80) {
      *value = v;
      cur_state_.cur_addr_++;
      return;
    }

    DecodeVarintFallback(value);
  }

  // Note: it changes cur_state_.cur_addr_
  void DecodeVarintFallback(uint32_t *value) noexcept {
    uint32_t result = 0;
    int count = 0;
    uint32_t b;

    do {
      b = *cur_state_.cur_addr_;
      result |= static_cast<uint32_t>(b & 0x7F) << (7 * count);
      cur_state_.cur_addr_++;
      ++count;
    } while (b & 0x80);

    *value = result;
  }

  const std::string *data_pointer_;
  const uint8_t *pl_addr_;
  const SkipIndex *skip_index_;
  int skip_span_;
  int total_postings_;

  struct State {
    int cur_posting_index_;
    DocIdType prev_doc_id_; // doc id of posting[cur_posting_index_ - 1]
    const uint8_t *cur_addr_;

    State(const uint8_t *cur_addr, int index, DocIdType id)
      :cur_addr_(cur_addr), cur_posting_index_(index), 
       prev_doc_id_(id) {}

    State(const State &rhs) = default;
    State & operator=(const State &rhs) = default;

    void Update(const uint8_t *cur_addr, int index, DocIdType id) {
      cur_addr_ = cur_addr;
      cur_posting_index_ = index;
      prev_doc_id_ = id;
    }
  };
  State cur_state_;

  struct PostingCache {
    uint32_t cur_content_bytes_;
    DocIdType cur_doc_id_;
    uint32_t cur_term_freq_;
    uint32_t offset_size_;
    const uint8_t *cur_offset_pairs_start_addr_;
    const uint8_t *cur_position_start_addr_;

    const uint8_t *next_posting_addr_;

    PostingCache() {}
    PostingCache(const PostingCache &rhs) = default;
    PostingCache & operator=(const PostingCache &rhs) = default;
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

  PostingListDeltaIterator Begin2() const {
    return PostingListDeltaIterator(
          &data_, 
          &skip_index_, 
          skip_span_, 
          Size(), 
          skip_index_.vec[0].prev_doc_id, 
          0);
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
