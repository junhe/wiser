#ifndef FLASH_ITERATORS_H
#define FLASH_ITERATORS_H

#include "packed_value.h"
#include "flash_engine_dumper.h"


class RawIntsIterator {
 public:
  RawIntsIterator(const uint8_t *buf, const SkipList &skiplist ): buf_(buf) {

  }

 private:
  // points to the start of a series packs and (maybe) a vints blob
  const uint8_t *buf_; 
};

#endif

