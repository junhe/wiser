#ifndef FLASH_ITERATORS_H
#define FLASH_ITERATORS_H

#include "packed_value.h"
#include "flash_engine_dumper.h"

enum class BlobFormat {PACKED_INTS, VINTS, NONE};

inline BlobFormat GetBlobFormat(const uint8_t *buf) {
  if (*buf & 0x80 != 0) {
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
      int blob_offset = cur_posting_index_ % PACK_SIZE;
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

  int CurBlobOffset() const {
    return cur_posting_index_ % PACK_SIZE;
  }

  // points to the start of a series packs and (maybe) a vints blob
  const uint8_t *buf_; 
  const SkipList &skip_list_;

  int cur_posting_index_ = 0;
  BlobFormat cur_iter_type_;
  
  PackedIntsIterator pack_ints_iter_;
  VIntsIterator vints_iter_;
};

#endif

