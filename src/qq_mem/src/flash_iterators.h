#ifndef FLASH_ITERATORS_H
#define FLASH_ITERATORS_H

#include "packed_value.h"
#include "flash_engine_dumper.h"

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
  if ((*buf & 0x80) == 0x80) {
    return BlobFormat::VINTS;
  } else {
    return BlobFormat::PACKED_INTS;
  }
}

class TermFreqIterator {
 public:
  TermFreqIterator(const uint8_t *buf, const SkipList &skip_list)
      : buf_(buf), skip_list_(skip_list), cur_iter_type_(BlobFormat::NONE) {
  }

  void SkipTo(int posting_index) {
    if (posting_index < cur_posting_index_) {
      LOG(FATAL) << "Posting index " << posting_index 
        << " is less than current posting index.";
    }

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
    }
  }

 private:
  void SetupBlob(int blob_index) {
    off_t blob_off = skip_list_[blob_index].file_offset_of_tf_bag;
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
  const SkipList &skip_list_;

  int cur_posting_index_ = 0;
  BlobFormat cur_iter_type_;
  
  PackedIntsIterator pack_ints_iter_;
  VIntsIterator vints_iter_;
};


class DocIdIterator {
 public:
  DocIdIterator(const uint8_t *buf, const SkipList &skip_list, 
      const int num_docids)
    : buf_(buf), skip_list_(skip_list), 
      cur_iter_type_(BlobFormat::NONE), num_docids_(num_docids) 
  {
    SkipTo(0);     
  }

  void SkipTo(int posting_index) {
    if (posting_index < cur_posting_index_) {
      LOG(FATAL) << "Posting index " << posting_index 
        << " is less than current posting index.";
    }

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

  int PostingIndex() const {
    return cur_posting_index_;
  }

  bool IsEnd() const {
    return cur_posting_index_ == num_docids_;
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
    }
  }

 private:
  // Get index of the blob whose prev_doc_id >= val
  int GetBlobIndexToGo(const uint32_t val) {
    int i = CurBlobIndex();
    const int last_index = LastBlobIndex();

    while (i + 1 <= last_index && skip_list_[i + 1].previous_doc_id < val) {
      i++;
    }

    return i;
  }

  int LastBlobIndex() const {
    return (num_docids_ - 1) / PACK_SIZE;
  }

  void SetupBlob(int blob_index) {
    off_t blob_off = skip_list_[blob_index].file_offset_of_docid_bag;
    const uint32_t &prev_doc_id = skip_list_[blob_index].previous_doc_id;
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
  const SkipList &skip_list_;

  int cur_posting_index_ = 0;
  BlobFormat cur_iter_type_;
  const int num_docids_;
  
  DeltaEncodedPackedIntsIterator pack_ints_iter_;
  DeltaEncodedVIntsIterator vints_iter_;
};


class CozyBoxIterator {
 public:
  CozyBoxIterator(const uint8_t *buf) 
    :buf_(buf), cur_iter_type_(BlobFormat::NONE), 
     cur_blob_off_(-1), cur_in_blob_index_(0) {}

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

  void Advance() {
    GoToCozyEntry(cur_blob_off_, cur_in_blob_index_ + 1);

    if (IsAtBlobEnd() == true) {
      GoToCozyEntry(cur_blob_off_ + CurBlobBytes(), 0);
    }
  }

  uint32_t Value() const {
    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      return pack_ints_iter_.Value();
    } else if (cur_iter_type_ == BlobFormat::VINTS) {
      return vints_iter_.Peek();
    }
  }

  bool IsAtBlobEnd() const {
    if (cur_iter_type_ == BlobFormat::PACKED_INTS) {
      return pack_ints_iter_.IsEnd();
    } else {
      return vints_iter_.IsEnd();
    }
  }

  off_t CurBlobOffset() const {
    return cur_blob_off_;
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

  const uint8_t *buf_; 
  BlobFormat cur_iter_type_;
  off_t cur_blob_off_;
  int cur_in_blob_index_;
  
  PackedIntsIterator pack_ints_iter_;
  VIntsIterator vints_iter_;
};


// Usage:
//   SkipTo(8);
//   tf = TermFreq();
//   for (int i = 0; i < tf; i++) {
//     Pop()
//   }
class PositionPostingBagIterator {
 public:
  PositionPostingBagIterator(const uint8_t *buf, const SkipList &skip_list,
      TermFreqIterator *tf_iter)
    : cozy_box_iter_(buf),
      skip_list_(skip_list), 
      tf_iter_(tf_iter)
  {}

  void SkipTo(int posting_bag) {
    int skip_interval = FindSkipInterval(posting_bag);

    GoToSkipPostingBag(skip_interval);
    int skip_bag = skip_interval * PACK_SIZE;
    int n_entries_between = NumCozyEntriesBetween(skip_bag, posting_bag);

    for (int i = 0; i < n_entries_between; i++) {
      cozy_box_iter_.Advance();
    }

    cur_posting_bag_ = posting_bag;
    prev_pos_ = 0;
  }

  int TermFreq() {
    tf_iter_->SkipTo(CurPostingBag());
    return tf_iter_->Value();
  }

  int CurPostingBag() const {
    return cur_posting_bag_;
  }

  uint32_t Pop() {
    uint32_t ret = prev_pos_ + cozy_box_iter_.Value(); 
    prev_pos_ = ret;
    cozy_box_iter_.Advance();
    return ret;
  }

 private:
  int CurSkipInterval() const {
    return cur_posting_bag_ / PACK_SIZE;
  }

  void GoToSkipPostingBag(const int skip_interval) {

    const SkipEntry &ent = skip_list_[skip_interval];
    const off_t &blob_off = ent.file_offset_of_pos_blob;
    const int &in_blob_index = ent.in_blob_index_of_pos_bag;

    cozy_box_iter_.GoToCozyEntry(blob_off, in_blob_index);
    cur_posting_bag_ = skip_interval * PACK_SIZE;
  }

  int FindSkipInterval(const int posting_bag) {
    int i = CurSkipInterval();
    while (i + 1 < skip_list_.NumEntries() && 
        skip_list_.StartPostingIndex(i + 1) <= posting_bag) 
    {
      i++;
    }
    return i;
  }

  int NumCozyEntriesBetween(int bag_a, int bag_b) {
    int n = 0;

    for (int i = bag_a; i < bag_b; i++) {
      tf_iter_->SkipTo(i);
      n += tf_iter_->Value();
    }

    return n;
  }

  CozyBoxIterator cozy_box_iter_;
  TermFreqIterator *tf_iter_;
  const SkipList &skip_list_;

  int cur_posting_bag_ = 0;
  uint32_t prev_pos_ = 0;
};

class OffsetPostingBagIterator {
 public:
  OffsetPostingBagIterator(const uint8_t *buf, const SkipList &skip_list,
      TermFreqIterator *tf_iter)
    : cozy_box_iter_(buf),
      skip_list_(skip_list), 
      tf_iter_(tf_iter)
  {}

  void SkipTo(int posting_bag) {
    int skip_interval = FindSkipInterval(posting_bag);

    GoToSkipPostingBag(skip_interval);
    int skip_bag = skip_interval * PACK_SIZE;
    int n_entries_between = NumCozyEntriesBetween(skip_bag, posting_bag);

    for (int i = 0; i < n_entries_between; i++) {
      cozy_box_iter_.Advance();
    }

    cur_posting_bag_ = posting_bag;
    prev_pos_ = 0;
  }

  int TermFreq() {
    tf_iter_->SkipTo(CurPostingBag());
    return tf_iter_->Value();
  }

  int CurPostingBag() const {
    return cur_posting_bag_;
  }

  uint32_t Pop() {
    uint32_t ret = prev_pos_ + cozy_box_iter_.Value(); 
    prev_pos_ = ret;
    cozy_box_iter_.Advance();
    return ret;
  }

 private:
  int CurSkipInterval() const {
    return cur_posting_bag_ / PACK_SIZE;
  }

  void GoToSkipPostingBag(const int skip_interval) {
    const SkipEntry &ent = skip_list_[skip_interval];
    const off_t &blob_off = ent.file_offset_of_offset_blob;
    const int &in_blob_index = ent.in_blob_index_of_offset_bag;

    cozy_box_iter_.GoToCozyEntry(blob_off, in_blob_index);
    cur_posting_bag_ = skip_interval * PACK_SIZE;
  }

  int FindSkipInterval(const int posting_bag) {
    int i = CurSkipInterval();
    while (i + 1 < skip_list_.NumEntries() && 
        skip_list_.StartPostingIndex(i + 1) <= posting_bag) 
    {
      i++;
    }
    return i;
  }

  int NumCozyEntriesBetween(int bag_a, int bag_b) {
    int n = 0;

    for (int i = bag_a; i < bag_b; i++) {
      tf_iter_->SkipTo(i);
      n += tf_iter_->Value() * 2;
    }

    return n;
  }

  CozyBoxIterator cozy_box_iter_;
  TermFreqIterator *tf_iter_;
  const SkipList &skip_list_;

  int cur_posting_bag_ = 0;
  uint32_t prev_pos_ = 0;
};


#endif

