#ifndef PACKED_VALUE_H
#define PACKED_VALUE_H

#include "utils.h"

int AppendToByte(long val, const int n_bits, uint8_t *buf, const int next_empty_bit);
int AppendValue(long val, int n_bits, uint8_t *buf, int next_empty_bit);
long ExtractBits(const uint8_t *buf, const int bit_start, const int n_bits);
int NumBitsInByte(int next_empty_bit);

class PackedIntsWriter {
 public:
  static const int PACK_SIZE = 128;

  void Add(long value) {
    values_.push_back(value);
    int n_bits = utils::NumOfBits(value);
    n_bits = n_bits == 0? 1 : n_bits;
    if (n_bits > max_bits_per_value_)
      max_bits_per_value_ = n_bits;
  }

  // Format
  // byte 0                1
  // data n_bits_per_value start of the block
  std::string Serialize() const {
    if (values_.size() != PACK_SIZE)
      LOG(FATAL) << "Number of values is not " << PACK_SIZE;

    constexpr int size = sizeof(long) * PACK_SIZE + 1;
    uint8_t buf[size];
    memset(buf, 0, size);

    if (max_bits_per_value_ > 64)
      LOG(FATAL) << "Max bits per value should not be larger than 64";

    buf[0] = max_bits_per_value_;

    int next_empty_bit = 8;
    for (auto &v : values_) {
      next_empty_bit = AppendValue(v, max_bits_per_value_, buf, next_empty_bit);
    }

    return std::string((char *)buf, (next_empty_bit + 7) / 8); // ceiling(next_empty_bit/8)
  }

  int MaxBitsPerValue() const {
    return max_bits_per_value_;
  }

 private:
  std::vector<long> values_;
  int max_bits_per_value_ = 0;
};


class PackedIntsReader {
 public:
  PackedIntsReader(const uint8_t *buf) {
    Reset(buf);
  }

  PackedIntsReader(){}

  void Reset(const uint8_t *buf) {
    buf_ = buf; 
    n_bits_per_value_ = buf[0];
  }

  long Get(const int index) {
    return ExtractBits(buf_ + 1, index * n_bits_per_value_, n_bits_per_value_);     
  }
  
  int NumBits() const {
    return n_bits_per_value_;
  }

 private:
  const uint8_t *buf_ = nullptr;
  int n_bits_per_value_ = 0;
};


class PackedIntsIterator {
 public:
  PackedIntsIterator(const uint8_t *buf) :reader_(buf) {}

 private:
  PackedIntsReader reader_;
};

#endif
