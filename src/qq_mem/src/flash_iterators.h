#ifndef FLASH_ITERATORS_H
#define FLASH_ITERATORS_H

#include "packed_value.h"
#include "file_dumper.h"
#include "bloom_filter.h"
#include "term_index.h"


DECLARE_bool(enable_prefetch);
DECLARE_int32(prefetch_threshold);


enum class BlobFormat {PACKED_INTS, VINTS, NONE};



inline std::string FormatString(const BlobFormat f) {
  if (f == BlobFormat::PACKED_INTS) {
    return "PACKED_INTS";
  } else if (f == BlobFormat::VINTS) {
    return "VINTS";
  } else if (f == BlobFormat::NONE) {
    return "NONE";
  } else {
    return "NOT IN ENUM";
  }

}

inline BlobFormat GetBlobFormat(const uint8_t *buf) {
  if ((*buf & 0xFF) == VINTS_FIRST_BYTE) {
    return BlobFormat::VINTS;
  } else if ((*buf & 0xFF) == PACK_FIRST_BYTE) {
    return BlobFormat::PACKED_INTS;
  } else {
    DLOG(FATAL) << "Wrong format";
  }

  return BlobFormat::NONE; // to suppress warning
}

class TermFreqIterator {
 public:
  TermFreqIterator() {}
  TermFreqIterator(const uint8_t *buf, const SkipList *skip_list) {
    Reset(buf, skip_list); 
  }

  void Reset(const uint8_t *buf, const SkipList *skip_list) {
    buf_ = buf;
    skip_list_ = skip_list;
    cur_iter_type_ = BlobFormat::NONE;
  }

  void SkipTo(int posting_index) {
    DLOG_IF(FATAL, posting_index < cur_posting_index_) 
      << "Posting index " << posting_index 
      << " is less than current posting index " << cur_posting_index_;

    int blob_index = posting_index / PACK_SIZE;
    int blob_offset = posting_index % PACK_SIZE;

    if (cur_iter_type_ == BlobFormat::NONE || CurBlobIndex() != blob_index) {
      SetupBlob(blob_index); 
    }

    if (cur_iter_type_ == BlobFormat::VINTS) {
      vints_iter_.SkipTo(blob_offset);
    } else {
      // Packed Ints
      pack_ints_iter_.SkipTo(blob_offset);
    }

    cur_posting_index_  = posting_index;
  }

  // You must call SKipTo() at least once before calling Value()
  uint32_t Value() const {
    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      return pack_ints_iter_.Value();
    } else if (cur_iter_type_ == BlobFormat::VINTS) {
      return vints_iter_.Peek();
    } else {
      DLOG(FATAL) << "blob has not been setup" << std::endl;
      return -1;
    }
  }

 private:
  void SetupBlob(int blob_index) {
    off_t blob_off = (*skip_list_)[blob_index].file_offset_of_tf_bag;
    const uint8_t *blob_buf = buf_ + blob_off;

    BlobFormat format = GetBlobFormat(blob_buf);
    cur_iter_type_ = format;

    if (format == BlobFormat::PACKED_INTS) {
      pack_ints_iter_.Reset(blob_buf);
    } else {
      vints_iter_.Reset(blob_buf);
    }
  }

  int CurBlobIndex() const {
    return cur_posting_index_ / PACK_SIZE;
  }

  // points to the start of a series packs and (maybe) a vints blob
  const uint8_t *buf_; 
  const SkipList *skip_list_;

  int cur_posting_index_ = 0;
  BlobFormat cur_iter_type_;
  
  PackedIntsIterator pack_ints_iter_;
  VIntsIterator vints_iter_;
};


class DocIdIterator {
 public:
  DocIdIterator() {}

  DocIdIterator(
      const uint8_t *buf, const SkipList *skip_list, const int num_docids)
  {
    Reset(buf, skip_list, num_docids);
  }

  void Reset(const uint8_t *buf, const SkipList *skip_list, const int num_docids)
  {
    buf_ = buf;
    skip_list_ = skip_list;
    cur_iter_type_ = BlobFormat::NONE;
    num_postings_ = num_docids;

    SkipTo(0);     
  }

  void SkipTo(int posting_index) {
    DLOG_IF(FATAL, posting_index < cur_posting_index_) 
        << "Posting index " << posting_index 
        << " is less than current posting index.";

    int blob_index = posting_index / PACK_SIZE;
    int blob_offset = posting_index % PACK_SIZE;

    if (cur_iter_type_ == BlobFormat::NONE || CurBlobIndex() != blob_index) {
      if (posting_index >= num_postings_) {
        SkipToEnd();
        return;
      } else {
        SetupBlob(blob_index); 
      }
    }

    if (cur_iter_type_ == BlobFormat::VINTS) {
      vints_iter_.SkipTo(blob_offset);
    } else {
      // Packed Ints
      pack_ints_iter_.SkipTo(blob_offset);
    }

    cur_posting_index_  = posting_index;
  }

  // IsEnd() will be true if you call SkipToEnd()
  void SkipToEnd() {
    cur_posting_index_ = num_postings_;
  }

  int PostingIndex() const {
    return cur_posting_index_;
  }

  bool IsEnd() const {
    return cur_posting_index_ == num_postings_;
  }

  void SkipForward(const uint32_t val) {
    int blob_to_go = GetBlobIndexToGo(val);
    int new_index_base = blob_to_go * PACK_SIZE;

    assert(blob_to_go >= CurBlobIndex());

    if (blob_to_go != CurBlobIndex()) {
      SkipTo(blob_to_go * PACK_SIZE); 
    }

    if (cur_iter_type_ == BlobFormat::VINTS) {
      vints_iter_.SkipForward(val);
      cur_posting_index_ = new_index_base + vints_iter_.Index();
    } else {
      // Packed Ints
      pack_ints_iter_.SkipForward(val);
      cur_posting_index_ = new_index_base + pack_ints_iter_.Index();
    }
  }

  void Advance() {
    SkipTo(PostingIndex() + 1);
  }

  uint32_t Value() const {
    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      return pack_ints_iter_.Value();
    } else if (cur_iter_type_ == BlobFormat::VINTS) {
      return vints_iter_.Peek();
    } else {
      DLOG(FATAL) << "blob has not been setup" << std::endl;
      return -1;
    }
  }

 private:
  // Get index of the blob whose prev_doc_id >= val
  int GetBlobIndexToGo(const uint32_t val) {
    int i = CurBlobIndex();
    const int last_index = LastBlobIndex();

    while (i + 1 <= last_index && (*skip_list_)[i + 1].previous_doc_id < val) {
      i++;
    }

    return i;
  }

  int LastBlobIndex() const {
    return (num_postings_ - 1) / PACK_SIZE;
  }

  void SetupBlob(int blob_index) {
    off_t blob_off = (*skip_list_)[blob_index].file_offset_of_docid_bag;
    const uint32_t &prev_doc_id = (*skip_list_)[blob_index].previous_doc_id;
    const uint8_t *blob_buf = buf_ + blob_off;

    BlobFormat format = GetBlobFormat(blob_buf);
    cur_iter_type_ = format;

    if (format == BlobFormat::PACKED_INTS) {
      pack_ints_iter_.Reset(blob_buf, prev_doc_id);
    } else {
      vints_iter_.Reset(blob_buf, prev_doc_id);
    }
  }

  int CurBlobIndex() const {
    return cur_posting_index_ / PACK_SIZE;
  }

  // points to the start of a series packs and (maybe) a vints blob
  const uint8_t *buf_; 
  const SkipList *skip_list_;

  int cur_posting_index_ = 0;
  BlobFormat cur_iter_type_;
  int num_postings_;
  
  DeltaEncodedPackedIntsIterator pack_ints_iter_;
  DeltaEncodedVIntsIterator vints_iter_;
};


// Iterface usage:
//    You must know how many items you need. For example, if you need 3: 
//
//    iter.GoTOCozyEntry(xx, xx);
//    v1 = iter.Value();
//
//    iter.Advance()
//    v2 = iter.Value()
//
//    iter.Advance()
//    v3 = iter.Value()
//
// Do not call GoToCozyEntry() and Advance() more than the number of 
// items you need. **CozyBoxIterator has no idea where the end of the 
// cozy box is.**
class CozyBoxIterator {
 public:
  CozyBoxIterator() {}
  CozyBoxIterator(const uint8_t *buf) {
    Reset(buf);
  }

  void Reset(const uint8_t *buf) {
    buf_ = buf;
    cur_iter_type_ = BlobFormat::NONE;
    cur_blob_off_ = -1;
    cur_in_blob_index_ = 0;
  }

  void GoToCozyEntry(off_t blob_off, int in_blob_index) {
    if (cur_iter_type_ == BlobFormat::NONE || cur_blob_off_ != blob_off) {
      SetupBlob(blob_off);
    }

    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      pack_ints_iter_.SkipTo(in_blob_index);
    } else {
      vints_iter_.SkipTo(in_blob_index);
    }
    cur_in_blob_index_ = in_blob_index;
  }

  // Must call GotoCozyEntry() before calling Advance()
  // Because Advance() needs cur_blob_off_ to be already set
  void Advance() {
    GoToCozyEntry(cur_blob_off_, cur_in_blob_index_ + 1);

    if (IsAtBlobEnd() == true) {
      GoToCozyEntry(cur_blob_off_ + CurBlobBytes(), 0);
    }
  }

  // You should never advance beyond the end. 
  // We do not do boundary checking here
  void AdvanceBy(int n_entries) {
    while (n_entries > 0) {
      if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
        int n_pack_remain = PACK_SIZE - cur_in_blob_index_;
        if (n_entries < n_pack_remain) {
          int to_index = cur_in_blob_index_ + n_entries;
          pack_ints_iter_.SkipTo(to_index);
          cur_in_blob_index_ = to_index;
          n_entries = 0;
        } else {
          AdvanceToNextBlob();
          n_entries -= n_pack_remain;
        }
      } else {
        Advance();
        --n_entries;
      }
    }
  }

  uint32_t Value() const {
    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      return pack_ints_iter_.Value();
    } else if (cur_iter_type_ == BlobFormat::VINTS) {
      return vints_iter_.Peek();
    } else {
      DLOG(FATAL) << "blob has not been setup" << std::endl;
      return -1;
    }
  }

  bool IsAtBlobEnd() const {
    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      return pack_ints_iter_.IsEnd();
    } else {
      return vints_iter_.IsEnd();
    }
  }

  void AdvanceToNextBlob() {
    GoToCozyEntry(cur_blob_off_ + CurBlobBytes(), 0);
  }

  off_t CurBlobOffset() const {
    return cur_blob_off_;
  }

  int CurInBlobIndex() const {
    return cur_in_blob_index_;
  }

  BlobFormat CurBlobType() const {
    return cur_iter_type_;
  }

  std::string Format() const {
    return FormatString(cur_iter_type_);
  }

 private:
  off_t CurBlobBytes() const {
    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      return pack_ints_iter_.SerializationSize();
    } else if (cur_iter_type_ == BlobFormat::VINTS) {
      return vints_iter_.SerializationSize();
    } else {
      DLOG(FATAL) << "blob has not been setup" << std::endl;
      return -1;
    }
  }

  void SetupBlob(const off_t blob_off) {
    const uint8_t *blob_buf = buf_ + blob_off;

    cur_iter_type_ = GetBlobFormat(blob_buf);

    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      pack_ints_iter_.Reset(blob_buf);
    } else {
      vints_iter_.Reset(blob_buf);
    }

    cur_blob_off_ = blob_off;
    cur_in_blob_index_ = 0;
  }

  const uint8_t *buf_ = nullptr; 
  BlobFormat cur_iter_type_ = BlobFormat::NONE;
  off_t cur_blob_off_ = 0;
  int cur_in_blob_index_ = 0;
  
  PackedIntsIterator pack_ints_iter_;
  VIntsIterator vints_iter_;
};


class InBagOffsetPairIterator: public OffsetPairsIteratorService {
 public:
  InBagOffsetPairIterator() {};
  InBagOffsetPairIterator(const CozyBoxIterator cozy_iter, const int term_freq) {
    Reset(cozy_iter, term_freq);
  }

  void Reset(const CozyBoxIterator cozy_iter, const int term_freq) {
    cozy_box_iter_ = cozy_iter;
    n_single_off_to_go_ = term_freq * 2;
    prev_pos_ = 0;
  }

  bool IsEnd() const {
    return n_single_off_to_go_ == 0;
  }

  void Pop(OffsetPair *pair) {
    std::get<0>(*pair) = SinglePop();
    std::get<1>(*pair) = SinglePop();
  }

 private:
  uint32_t SinglePop() {
    uint32_t pos = prev_pos_ + cozy_box_iter_.Value();
    prev_pos_ = pos;
    n_single_off_to_go_--; 

    // Cozy box does not know its end, but we do know how many 
    // positions we need to pop in this class, so we use this 
    // info in this higher-level class to make the decision.
    if (n_single_off_to_go_ > 0)
      cozy_box_iter_.Advance();

    return pos;
  }

  CozyBoxIterator cozy_box_iter_;
  uint32_t prev_pos_;
  int n_single_off_to_go_;
};


class PosAndOffPostingBagIteratorBase {
 public:
  PosAndOffPostingBagIteratorBase() {}

  void Reset(
      const uint8_t *buf, const SkipList *skip_list, TermFreqIterator tf_iter) {
    cozy_box_iter_.Reset(buf);
    skip_list_ = skip_list;
    tf_iter_ = tf_iter;
  }

  int CurPostingBag() const {
    return cur_posting_bag_;
  }

  void SkipTo(int posting_bag) {
    DLOG_IF(FATAL, posting_bag < cur_posting_bag_) << "posting bag " << posting_bag 
        << " is smaller than cur_posting_bag_ " << cur_posting_bag_;

    if (cozy_box_iter_.CurBlobType() == BlobFormat::NONE || 
        posting_bag / SKIP_INTERVAL > CurSkipInterval()) {
      // brand new position iterator, init the cozy iter
      JumpToPostingBag(posting_bag);
    } else {
      // just advance cozy iterator from last place, not jumpping
      int n_entries = NumCozyEntriesBetween(cur_posting_bag_, posting_bag);
      AdvanceByCozyEntries(n_entries);
    }

    cur_posting_bag_ = posting_bag;
  }

  const CozyBoxIterator &GetCozyBoxIterator() const {
    return cozy_box_iter_;
  }

  int TermFreq() {
    tf_iter_.SkipTo(CurPostingBag());
    return tf_iter_.Value();
  }

 protected:
  virtual int NumCozyEntriesBetween(int bag_a, int bag_b) = 0;
  virtual off_t GetEntryBlobOff(const SkipEntry &ent) = 0;
  virtual int GetEntryInBlobIndex(const SkipEntry &ent) = 0;

  void GoToSkipPostingBag(const int skip_interval) {
    const SkipEntry &ent = (*skip_list_)[skip_interval];
    // const off_t &blob_off = ent.file_offset_of_pos_blob;
    // const int &in_blob_index = ent.in_blob_index_of_pos_bag;
    const off_t blob_off = GetEntryBlobOff(ent);
    const int in_blob_index = GetEntryInBlobIndex(ent);

    cozy_box_iter_.GoToCozyEntry(blob_off, in_blob_index);
    cur_posting_bag_ = skip_interval * PACK_SIZE;
  }

  int CurSkipInterval() const {
    return cur_posting_bag_ / PACK_SIZE;
  }

  // TODO: why not just do posting_bag / PACK_SIZE ?
  int FindSkipInterval(const int posting_bag) {
    int i = CurSkipInterval();
    while (i + 1 < skip_list_->NumEntries() && 
        skip_list_->StartPostingIndex(i + 1) <= posting_bag) 
    {
      i++;
    }
    return i;
  }

  void JumpToPostingBag(int posting_bag) {
    int skip_interval = FindSkipInterval(posting_bag);
    GoToSkipPostingBag(skip_interval);
    int skip_bag = skip_interval * PACK_SIZE;
    int n_entries_between = NumCozyEntriesBetween(skip_bag, posting_bag);
    AdvanceByCozyEntries(n_entries_between);
  }

  void AdvanceByCozyEntries(int n_entries) {
    cozy_box_iter_.AdvanceBy(n_entries);
  }

  CozyBoxIterator cozy_box_iter_;
  TermFreqIterator tf_iter_;
  const SkipList *skip_list_;

  int cur_posting_bag_ = 0;
  int cur_term_freq_;
};



// Usage:
//   SkipTo(8);
//   in_bag_iter = InBagPositionBegin()
//   while (in_bag_iter.IsEnd()) {
//     in_bag_iter.Pop()
//   }
class PositionPostingBagIterator :public PosAndOffPostingBagIteratorBase {
 public:
  PositionPostingBagIterator() {}
  PositionPostingBagIterator(
      const uint8_t *buf, const SkipList *skip_list, TermFreqIterator tf_iter) {
    Reset(buf, skip_list, tf_iter);
  }

  const CozyBoxIterator &GetCozyBoxIterator() const {
    return cozy_box_iter_;
  }

  void SkipTo(int posting_bag) {
    DLOG_IF(FATAL, posting_bag < cur_posting_bag_) << "posting bag " << posting_bag 
        << " is smaller than cur_posting_bag_ " << cur_posting_bag_;

    if (cozy_box_iter_.CurBlobType() == BlobFormat::NONE || 
        posting_bag / SKIP_INTERVAL > CurSkipInterval()) {
      // brand new position iterator, init the cozy iter
      JumpToPostingBag(posting_bag);
    } else {
      // just advance cozy iterator from last place, not jumpping
      int n_entries = NumCozyEntriesBetween(cur_posting_bag_, posting_bag);
      n_entries -= n_advanced_in_bag_;
      AdvanceByCozyEntries(n_entries);
    }

    cur_posting_bag_ = posting_bag;

    prev_pos_in_bag_ = 0;
    cur_term_freq_ = TermFreq();
    n_popped_in_bag_ = 0;
    n_advanced_in_bag_ = 0;
  }

  uint32_t PopInBag() {
    uint32_t pos = prev_pos_in_bag_ + cozy_box_iter_.Value();
    prev_pos_in_bag_ = pos;
    n_popped_in_bag_++;

    if (n_popped_in_bag_ < cur_term_freq_) {
      cozy_box_iter_.Advance();
      n_advanced_in_bag_++;
    }

    return pos;
  }

  bool IsEndInBag() {
    return n_popped_in_bag_ >= cur_term_freq_;
  }

 private:
  off_t GetEntryBlobOff(const SkipEntry &ent) override {
    return ent.file_offset_of_pos_blob;
  }

  int GetEntryInBlobIndex(const SkipEntry &ent) override {
    return ent.in_blob_index_of_pos_bag;
  }

  int NumCozyEntriesBetween(int bag_a, int bag_b) {
    int n = 0;

    for (int i = bag_a; i < bag_b; i++) {
      tf_iter_.SkipTo(i);
      n += tf_iter_.Value();
    }

    return n;
  }

  int n_popped_in_bag_; 
  int n_advanced_in_bag_;
  uint32_t prev_pos_in_bag_ = 0;
  int cur_term_freq_;
};


// A simple wrapper of Positionpostingbagiterator for 
// clear interfaces
class InBagPositionIterator {
 public:
  InBagPositionIterator() {}

  InBagPositionIterator(PositionPostingBagIterator *it) {
    Reset(it);
  }

  void Reset(PositionPostingBagIterator *it) {
    pos_bag_iter_ = it;
  }

  uint32_t Pop() {
    return pos_bag_iter_->PopInBag();
  }

  bool IsEnd() const {
    return pos_bag_iter_->IsEndInBag();
  }

 private:
  PositionPostingBagIterator *pos_bag_iter_;
};





class OffsetPostingBagIterator :public PosAndOffPostingBagIteratorBase {
 public:
  OffsetPostingBagIterator() {}
  OffsetPostingBagIterator(
      const uint8_t *buf, const SkipList *skip_list, TermFreqIterator tf_iter)
  {
    Reset(buf, skip_list, tf_iter);
  }

  InBagOffsetPairIterator InBagOffsetPairBegin() {
    return InBagOffsetPairIterator(cozy_box_iter_, TermFreq());
  }

  std::unique_ptr<OffsetPairsIteratorService> InBagOffsetPairPtr() {
    std::unique_ptr<OffsetPairsIteratorService> p(new
      InBagOffsetPairIterator(cozy_box_iter_, TermFreq()));
    return p;
  }

 private:
  off_t GetEntryBlobOff(const SkipEntry &ent) override {
    return ent.file_offset_of_offset_blob;
  }

  int GetEntryInBlobIndex(const SkipEntry &ent) override {
    return ent.in_blob_index_of_offset_bag;
  }

  int NumCozyEntriesBetween(int bag_a, int bag_b) override {
    int n = 0;

    for (int i = bag_a; i < bag_b; i++) {
      tf_iter_.SkipTo(i);
      n += tf_iter_.Value() * 2;
    }

    return n;
  }
};


// Iterating offset pairs in a posting bag.
// Because it needs to jump in cozy box for offsets, it needs
// OffsetPostingBagIterator inside.
class LazyBoundedOffsetPairIterator: public OffsetPairsIteratorService {
 public:
  LazyBoundedOffsetPairIterator(
      int posting_index, 
      const OffsetPostingBagIterator off_bag_iter) 
    :posting_index_(posting_index), off_bag_iter_(off_bag_iter) 
  {
    prev_off_ = 0;
    is_cozy_iter_initialized_ = false;
  }

  bool IsEnd() const override {
    // Assuming TF always > 0
    if (is_cozy_iter_initialized_ == false)
      return false;

    return n_single_off_to_go_ == 0;
  }

  void Pop(OffsetPair *pair) override {
    if (is_cozy_iter_initialized_ == false)
      InitializeCozy();

    std::get<0>(*pair) = SinglePop();
    std::get<1>(*pair) = SinglePop();
  }
 
 private:
  void InitializeCozy() {
    off_bag_iter_.SkipTo(posting_index_);
    n_single_off_to_go_ = off_bag_iter_.TermFreq() * 2;

    cozy_box_iter_ = off_bag_iter_.GetCozyBoxIterator();

    is_cozy_iter_initialized_ = true;
  }

  uint32_t SinglePop() {
    uint32_t off = prev_off_ + cozy_box_iter_.Value();
    prev_off_ = off;
    n_single_off_to_go_--; 

    // Cozy box does not know its end, but we do know how many 
    // positions we need to pop in this class, so we use this 
    // info in this higher-level class to make the decision.
    if (n_single_off_to_go_ > 0)
      cozy_box_iter_.Advance();

    return off;
  }

  const int posting_index_;
  OffsetPostingBagIterator off_bag_iter_;

  CozyBoxIterator cozy_box_iter_;
  uint32_t prev_off_ = 0;
  int n_single_off_to_go_ = 0;
  bool is_cozy_iter_initialized_;
};


// Usage:
//  Reset()
//  LoadSkipList()
//  SkipTo(x)
class BloomFilterColumnReader {
 public:
  void Reset(const uint8_t *pl_buf, 
      const uint8_t *bloom_skip_list_buf, int bit_array_bytes) 
  {
    pl_buf_ = pl_buf;
    skip_list_buf_ = bloom_skip_list_buf;
    cur_posting_index_ = -1;
    bit_array_bytes_ = bit_array_bytes;
  }

  void SkipTo(int posting_index) {
    int box_index = posting_index / PACK_SIZE;

    if (cur_posting_index_ == -1 || CurBoxIndex() != box_index) {
      SetupBox(box_index);
    }
    cur_posting_index_ = posting_index;
  }

  const uint8_t *BitArray() {
    int box_offset =  cur_posting_index_ % PACK_SIZE;
    return box_iter_.GetBitArray(box_offset); 
  }

  void LoadSkipList() {
    skip_list_.Load(skip_list_buf_); 
  }

 private:
  int CurBoxIndex() const {
    return cur_posting_index_ / PACK_SIZE;
  }

  void SetupBox(int box_index) {
    const uint8_t *box_buf = pl_buf_ + skip_list_[box_index];
    box_iter_.Reset(box_buf, bit_array_bytes_);
  }

  const uint8_t *pl_buf_;
  const uint8_t *skip_list_buf_;
  
  int cur_posting_index_;
  int bit_array_bytes_;
  
  BloomSkipList skip_list_;
  BloomBoxIterator box_iter_;
};


struct VacuumHeader {
  void Load(const uint8_t *buf) {
    // Head is at the first 100 bytes of the .vacuum file  
    int len;
    uint32_t val;

    LOG_IF(FATAL, buf[0] != VACUUM_FIRST_BYTE) 
      << "Vacuum's first byte is wrong";
    buf++;
  
    // Bloom begin
    len = utils::varint_decode_uint32((const char *)buf, 0, &val);
    has_bloom_filters_begin = val;
    buf += len;

    len = utils::varint_decode_uint32((const char *)buf, 0, 
        &bit_array_bytes_begin);
    buf += len;

    len = utils::varint_decode_uint32((const char *)buf, 0,
        &expected_entries_begin);
    buf += len;

    bloom_ratio_begin = utils::DeserializeFloat((const char *)buf);
    buf += sizeof(float);

    // Bloom end
    len = utils::varint_decode_uint32((const char *)buf, 0, &val);
    has_bloom_filters_end = val;
    buf += len;

    len = utils::varint_decode_uint32((const char *)buf, 0, 
        &bit_array_bytes_end);
    buf += len;

    len = utils::varint_decode_uint32((const char *)buf, 0,
        &expected_entries_end);
    buf += len;

    bloom_ratio_end = utils::DeserializeFloat((const char *)buf);
    buf += sizeof(float);

    // Just not to break the current tests
    has_bloom_filters = has_bloom_filters_end;
    bit_array_bytes = bit_array_bytes_end;
    expected_entries = expected_entries_end;
    bloom_ratio = bloom_ratio_end;
  }

  bool has_bloom_filters_begin;
  uint32_t bit_array_bytes_begin;
  uint32_t expected_entries_begin;
  float bloom_ratio_begin;

  bool has_bloom_filters_end;
  uint32_t bit_array_bytes_end;
  uint32_t expected_entries_end;
  float bloom_ratio_end;

  bool has_bloom_filters;
  uint32_t bit_array_bytes;
  uint32_t expected_entries;
  float bloom_ratio;
};


// It will iterate postings in a posting list
class VacuumPostingListIterator {
 public:
  VacuumPostingListIterator() {
  }

  VacuumPostingListIterator(const uint8_t *file_data, 
      const VacuumHeader *header, const TermIndexResult &result) {
    ResetWithZoneInfo(file_data, header, result);
  }

  void ResetWithZoneInfo(const uint8_t *file_data, 
      const VacuumHeader *header, const TermIndexResult &result) {
    int len;
    file_data_ = file_data;
    term_index_result_ = result;
    const off_t offset = result.GetPostingListOffset();
    const uint8_t *buf = file_data + offset;

    header_ = header;

    // first byte is the magic number
    DLOG_IF(FATAL, (buf[0] & 0xFF) != POSTING_LIST_FIRST_BYTE)
      << "Magic number for posting list is wrong: " 
      << std::hex << buf[0];
    buf += 1;

    // second item in the posting list is the doc freq (n_postings_)
    len = utils::varint_decode_uint8(buf, 0, &n_postings_);
    buf += len;

    // third item is the location of bloom skip list
    uint32_t bloom_offset;
    const uint8_t *sec_buf = buf;
    if (header_->has_bloom_filters == true) {
      // for bloom begin
      len = utils::varint_decode_uint32((const char *)buf, 0, &bloom_offset);
      buf += len;
      bloom_begin_reader_.Reset(file_data_ + offset, 
                          file_data_ + offset + bloom_offset, 
                          header->bit_array_bytes);

      // for bloom end
      len = utils::varint_decode_uint32((const char *)buf, 0, &bloom_offset);
      buf += len;
      bloom_end_reader_.Reset(file_data_ + offset, 
                          file_data_ + offset + bloom_offset, 
                          header->bit_array_bytes);

      bloom_set(&bloom_, header->expected_entries, header->bloom_ratio, 
          nullptr);
    }
    buf = sec_buf + 8; // we reserved 4 bytes for this value

    // fourth item is the start of skip list
    skip_list_ = std::shared_ptr<SkipList>(new SkipList()); 
    skip_list_->Load(buf);

    doc_id_iter_.Reset(file_data_, skip_list_.get(), n_postings_);
    tf_iter_.Reset(file_data_, skip_list_.get());
    pos_bag_iter_.Reset(file_data_, skip_list_.get(), tf_iter_);
    off_bag_iter_.Reset(file_data_, skip_list_.get(), tf_iter_);

    is_bloom_skip_list_loaded_ = false;
  }

  //term_index_result_ must have been set
  bool ShouldPrefetch() {
    DLOG_IF(FATAL, term_index_result_.IsEmpty() == true) << "term_index_result_ is empty";
    return  FLAGS_enable_prefetch == true &&
      term_index_result_.GetNumPagesInPrefetchZone() > (uint32_t)FLAGS_prefetch_threshold;
  }
  
  //Note that file_data_ and term_index_result_ must have been set
  void Prefetch() const {
    DLOG_IF(FATAL, term_index_result_.IsEmpty() == true);

	  const void * pl_addr = file_data_ + term_index_result_.GetPostingListOffset();
    size_t zone_bytes = term_index_result_.GetNumPagesInPrefetchZone() * 4 * KB;

    pl_addr = (char *)pl_addr - ((off_t)pl_addr % (4*KB));
    int ret = madvise((void *) pl_addr, zone_bytes, MADV_SEQUENTIAL);

    if (ret == -1) {
      perror("Fail to do madvise");
      DLOG(FATAL) << "Failed to prefetch.";
    }
  }

  std::string SkipListString() const {
    return skip_list_->ToStr();
  }

  DocIdType DocId() {
    return doc_id_iter_.Value();
  }

  int TermFreq () {
    tf_iter_.SkipTo(doc_id_iter_.PostingIndex());
    return tf_iter_.Value();
  }

  int HasPriorTerm(const std::string &term) {
    return HasTerm(&bloom_begin_reader_, term);
  }

  int HasNextTerm(const std::string &term) {
    return HasTerm(&bloom_end_reader_, term);
  }

  void AssignPositionBegin(InBagPositionIterator *in_bag_iter) {
    pos_bag_iter_.SkipTo(doc_id_iter_.PostingIndex());
    in_bag_iter->Reset(&pos_bag_iter_); 
  }

  std::unique_ptr<OffsetPairsIteratorService> OffsetPairsBegin() {
    std::unique_ptr<OffsetPairsIteratorService> p(
        new LazyBoundedOffsetPairIterator(
          doc_id_iter_.PostingIndex(), off_bag_iter_));
    return p;
  }

  void Advance() {
    doc_id_iter_.Advance();
  }

  void SkipForward(const uint32_t doc_id) {
    doc_id_iter_.SkipForward(doc_id);
  }
  
  int PostingIndex() {
    return doc_id_iter_.PostingIndex();
  }

  bool IsEnd() {
    return doc_id_iter_.IsEnd();
  }

  int Size() const {
    return n_postings_;
  }

  const std::string &Term() {
    return term_index_result_.Key();
  }

  std::unordered_map<std::string, std::size_t> GetSizeStats() {
    std::unordered_map<std::string, std::size_t> ret;

    ret["skiplist"] = skip_list_->GetSkipListBytes();
    ret["docid"] = skip_list_->GetDocIdBytes();
    ret["tf"] = skip_list_->GetTfBytes();
    ret["pos"] = skip_list_->GetPosBytes();
    ret["off"] = skip_list_->GetOffsetBytes();

    return ret;
  }

 private:
  int HasTerm(BloomFilterColumnReader *reader, const std::string &term) {
    if (header_->has_bloom_filters == true) {
      if (is_bloom_skip_list_loaded_ == false) {
        is_bloom_skip_list_loaded_ = true;
        reader->LoadSkipList();
      }

      reader->SkipTo(doc_id_iter_.PostingIndex());
      const uint8_t *bf = reader->BitArray();
      if (bf == nullptr) {
        return BLM_NOT_PRESENT;
      } else {
        // WARNING: casting from const to non-const
        bloom_.bf = (unsigned char *) bf;
        return CheckBloom(&bloom_, term);
      }
    } else {
      return BLM_MAY_PRESENT;
    }
  }

  const uint8_t *file_data_;
  TermIndexResult term_index_result_;
  const VacuumHeader *header_;
  bool is_bloom_skip_list_loaded_;
  
  uint32_t n_postings_;

  // iterators
  DocIdIterator doc_id_iter_;
  TermFreqIterator tf_iter_;
  PositionPostingBagIterator pos_bag_iter_;
  OffsetPostingBagIterator off_bag_iter_;
  BloomFilterColumnReader bloom_begin_reader_;
  BloomFilterColumnReader bloom_end_reader_;

  Bloom bloom_;

  std::shared_ptr<SkipList> skip_list_;
};


#endif

