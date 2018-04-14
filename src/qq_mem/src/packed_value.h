#ifndef PACKED_VALUE_H
#define PACKED_VALUE_H

#include "compression.h"
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

    // set the highest bit to 0 to distinguish from VInts 
    buf[0] = max_bits_per_value_ & 0x7F; 

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

  long Get(const int index) const {
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
  PackedIntsIterator() {}

  PackedIntsIterator(const uint8_t *buf) {
    Reset(buf); 
  }

  void Reset(const uint8_t *buf) {
    index_ = 0;
    reader_.Reset(buf);
  }

  void SkipTo(int index) {
    index_ = index;
  }

  void Advance() {
    index_++;
  }

  bool IsEnd() const {
    return index_ == PackedIntsWriter::PACK_SIZE;
  }

  long Value() const {
    return reader_.Get(index_);
  }

  int Index() const {
    return index_;
  }

 private:
  int index_;
  PackedIntsReader reader_;
};


class DeltaEncodedPackedIntsIterator {
 public:
  DeltaEncodedPackedIntsIterator() {}

  DeltaEncodedPackedIntsIterator(const uint8_t *buf, const long pre_pack_int) {
    Reset(buf, pre_pack_int); 
  }

  void Reset(const uint8_t *buf, const long pre_pack_int) {
    index_ = 0;
    prev_value_ = pre_pack_int;
    reader_.Reset(buf);
  }

  void Advance() {
    prev_value_ = Value();
    index_++;
  }

  void SkipTo(int index) {
    while (index_ < index) {
      Advance(); 
    }
  }

  void SkipForward(uint32_t val) {
    while (IsEnd() == false && Value() < val) {
      Advance();
    }
  }

  // If IsEnd() is true, Value() is UNreadable
  bool IsEnd() const {
    return index_ == PackedIntsWriter::PACK_SIZE;
  }

  long Value() const {
    return prev_value_ + reader_.Get(index_);
  }

  int Index() const {
    return index_;
  }

 private:
  int index_;
  long prev_value_;
  PackedIntsReader reader_;
};


class VIntsWriter {
 public:
  void Append(uint32_t val) {
    varint_buf_.Append(val);
  }

  int IntsSize() const {
    return varint_buf_.Size();
  }

  int SerializationSize() const {
    return varint_buf_.Size() + 2;
  }

  std::string Serialize() const {
    VarintBuffer buf;
    buf.Append(0x80); // to distinguish with PackedInts
    buf.Append(varint_buf_.Size());
    buf.Append(varint_buf_.Data());

    return buf.Data();
  }

 private:
  VarintBuffer varint_buf_;
};


class VIntsIterator {
 public:
  VIntsIterator() {}

  VIntsIterator(const uint8_t *buf) {
    Reset(buf);
  }

  void Reset(const uint8_t *buf) {
    int len = utils::varint_decode_chars((char *)buf, 0, &magic_);
    buf += len;

    if (magic_ != 0x80) {
      LOG(FATAL) << "Magic number is not right for this VInts";
    }

    len = utils::varint_decode_chars((char *)buf, 0, &varint_bytes_);
    buf += len;

    varint_iter_.Reset((char *)buf, 0, varint_bytes_);

    next_index_ = 0;
  }

  bool IsEnd() const {
    return varint_iter_.IsEnd();
  }

  uint32_t Pop() {
    next_index_++;
    return varint_iter_.Pop();
  }

  uint32_t Peek() const {
    return varint_iter_.Peek(); 
  }

  int Index() const {
    return next_index_;
  }

  // SkipTo(0); Peek() returns data with index 0 (the first item)
  // SkipTo(3); Peek() returns data with index 3 (the fourth item)
  void SkipTo(int index) {
    while (Index() < index) {
      Pop();
    }
  }

 private:
  int next_index_;
  uint32_t magic_; 
  uint32_t varint_bytes_;
  VarintIteratorEndBound varint_iter_;
};


class DeltaEncodedVIntsIterator {
 public:
  DeltaEncodedVIntsIterator() {}
  DeltaEncodedVIntsIterator(const uint8_t *buf, const long pre_int) {
    Reset(buf, pre_int);
  }

  void Reset(const uint8_t *buf, const uint32_t pre_int) {
    raw_iter_.Reset(buf);
    prev_value_ = pre_int;
  }

  bool IsEnd() const {
    return raw_iter_.IsEnd();
  }

  int Index() const {
    return raw_iter_.Index();
  }

  void SkipForward(const uint32_t val) {
    while (IsEnd() == false && Peek() < val) {
      Pop();
    }
  }

  uint32_t Pop() {
    prev_value_ = prev_value_ + raw_iter_.Pop();    
    return prev_value_;
  }

  uint32_t Peek() const {
    return prev_value_ + raw_iter_.Peek();    
  }

  void SkipTo(int index) {
    while (Index() < index) {
      Pop();
    }
  }

 private:
  VIntsIterator raw_iter_; 
  long prev_value_;
};

#endif
