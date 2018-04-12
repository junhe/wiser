#ifndef FLASH_ITERATORS_H
#define FLASH_ITERATORS_H

#include "packed_value.h"
#include "flash_engine_dumper.h"


class RawIntsIterator {
 public:
  RawIntsIterator(const uint8_t *buf, const SkipList &skip_list)
      : buf_(buf), skip_list_(skip_list) {
  }

  long & operator [] (int int_index) {
  }

 private:
  // points to the start of a series packs and (maybe) a vints blob
  const uint8_t *buf_; 
  const SkipList &skip_list_;
  PackedIntsReader pack_ints_reader_;
};

#endif

