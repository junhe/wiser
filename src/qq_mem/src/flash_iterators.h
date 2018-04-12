#ifndef FLASH_ITERATORS_H
#define FLASH_ITERATORS_H

#include "packed_value.h"
#include "flash_engine_dumper.h"


class RawIntsIterator {
 public:
  RawIntsIterator(const uint8_t *buf, const SkipList &skip_list)
      : buf_(buf), skip_list_(skip_list) {
  }

  void SkipTo(int posting_index) {
    int blob_index = posting_index / PACK_SIZE;
    int blob_offset = posting_index % PACK_SIZE;
  }

 private:
  // points to the start of a series packs and (maybe) a vints blob
  const uint8_t *buf_; 
  const SkipList &skip_list_;
  int cur_posting_index_ = 0;

  PackedIntsReader pack_ints_reader_;
  VIntsReader vints_reader_;
};

#endif

