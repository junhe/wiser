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
      : buf_(buf), skip_list_(skip_list), cur_reader_type_(BlobFormat::NONE) {
  }

  void SkipTo(int posting_index) {
    int blob_index = posting_index / PACK_SIZE;
    int blob_offset = posting_index % PACK_SIZE;

    if (posting_index < cur_posting_index_) {
      LOG(FATAL) << "Posting index " << posting_index 
        << " is less than current posting index.";
    }

    /*
    if reader is not for blob_index or reader not exist
      setup reader: 

    if reader is pack reader
      read by pack reader
    else
      read by vint reader
    */
    if (cur_reader_type_ == BlobFormat::NONE || CurBlobIndex() != blob_index) {
      SetupBlob(blob_index); 
    }

  }

 private:
  void SetupBlob(int blob_index) {
    off_t blob_off = skip_list_[blob_index].file_offset_of_tf_bag;
    const uint8_t *blob_buf = buf_ + blob_off;

    BlobFormat format = GetBlobFormat(blob_buf);

    if (format == BlobFormat::PACKED_INTS) {
      pack_ints_reader_.Reset(blob_buf);
    } else {
      vints_reader_.Reset(blob_buf);
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
  BlobFormat cur_reader_type_;
  
  PackedIntsReader pack_ints_reader_;
  VIntsReader vints_reader_;
};

#endif

