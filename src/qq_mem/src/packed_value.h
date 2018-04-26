#ifndef PACKED_VALUE_H
#define PACKED_VALUE_H

#include "compression.h"
#include "utils.h"

extern "C" {
#include "bitpacking.h"
}


#define PACK_FIRST_BYTE 0xD5
#define VINTS_FIRST_BYTE 0x9B

#define PACK_ITEM_CNT 128




inline int NumBitsInByte(int next_empty_bit) {
  return 8 - (next_empty_bit % 8);
}

inline int NumDataBytesInPack(int bits_per_value) {
  return (bits_per_value * PACK_ITEM_CNT + 7) / 8;
}

// Append the rightmost n_bits of val to next_empty_bit in buf
// val must have only n_bits
// You must garantee that val will not overflow the char
// Return: the next empty bit's bit index
inline int AppendToByte(const long val, const int n_bits, uint8_t *buf, const int next_empty_bit) {
  int byte_index = next_empty_bit / 8;
  int in_byte_off = next_empty_bit % 8;
  int n_shift = 8 - n_bits - in_byte_off;

  buf[byte_index] |= val << n_shift;

  return next_empty_bit + n_bits;
}

// Append the right-most n_bits to next_empty_bit in buf
// val must have only n_bits
inline int AppendValue(long val, int n_bits, uint8_t *buf, int next_empty_bit) {
  int n_bits_remain = n_bits;

  while (n_bits_remain > 0) {
    const int n_bits_in_byte = NumBitsInByte(next_empty_bit);  
    const int n_bits_to_send = n_bits_remain < n_bits_in_byte? n_bits_remain : n_bits_in_byte;
    const int n_shift = n_bits_remain - n_bits_to_send;
    long v = val >> n_shift; 

    next_empty_bit = AppendToByte(v, n_bits_to_send, buf, next_empty_bit);

    n_bits_remain = n_bits_remain - n_bits_to_send;
    int n = sizeof(long) * 8 - n_bits_remain;
    val = (val << n) >> n; // set the left n bits to 0
  }

  return next_empty_bit;
}


inline long ExtractBits(const uint8_t *buf, const int bit_start, const int n_bits) {
  long val = 0;  
  int n_bits_remain = n_bits;
  int next_bit = bit_start;

  while (n_bits_remain > 0) {
    const int byte_index = next_bit / 8;
    const int in_byte_off = next_bit % 8;
    const int n_unread_bits = 8 - in_byte_off;
    const int n_bits_to_read = 
      n_bits_remain < n_unread_bits?  n_bits_remain : n_unread_bits;

    // number of bits to the left of the current bits
    const int n_bits_on_left = 64 - n_bits + (next_bit - bit_start); 
    uint8_t tmp = ((buf[byte_index] << in_byte_off) & 0xFF) >> (8 - n_bits_to_read);

    val |= tmp << (64 - n_bits_to_read - n_bits_on_left);

    n_bits_remain -= n_bits_to_read;
    next_bit += n_bits_to_read;
  }

  return val;
}


// Using the little int packer lib to pack bits
class LittlePackedIntsWriter {
 public:
  static constexpr int HEADER_BYTES = 2;
  static constexpr int BUF_SIZE = HEADER_BYTES + PACK_ITEM_CNT * sizeof(uint32_t);

  // although value is `long`, we only support uint32_t
  void Add(long value) {
    values_[add_to_index_++] = value;

    int n_bits = utils::NumOfBits(value);
    n_bits = n_bits == 0? 1 : n_bits;
    if (n_bits > max_bits_per_value_)
      max_bits_per_value_ = n_bits;
  }

  int MaxBitsPerValue() const {
    return max_bits_per_value_;
  }

  std::string Serialize() {
    LOG_IF(FATAL, add_to_index_ != PACK_ITEM_CNT) 
      << "Number of values is not " << PACK_ITEM_CNT;

    uint8_t buf[BUF_SIZE];
    SetHeader(buf);

    turbopack32(values_, PACK_ITEM_CNT, max_bits_per_value_, buf + HEADER_BYTES);
    return std::string(
        (char *)buf, HEADER_BYTES + NumDataBytesInPack(max_bits_per_value_));
  }

 private:
  void SetHeader(uint8_t *buf) {
    // set bit 00__ ____ to distinguish from VINTS
    buf[0] = PACK_FIRST_BYTE;
    buf[1] = max_bits_per_value_;
  }

  uint32_t values_[PACK_ITEM_CNT];
  int max_bits_per_value_ = 0;
  int add_to_index_ = 0;
};


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

    constexpr int size = sizeof(long) * PACK_SIZE + 2;
    uint8_t buf[size];
    memset(buf, 0, size);

    if (max_bits_per_value_ > 64)
      LOG(FATAL) << "Max bits per value should not be larger than 64";

    // set bit 00__ ____ to distinguish from VINTS
    buf[0] = PACK_FIRST_BYTE;
    buf[1] = max_bits_per_value_;

    int next_empty_bit = 16;
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


// Usage:
//
// Reset(buf);
// DecodeToCache()
// Get(i)
class LittlePackedIntsReader {
 public:
  LittlePackedIntsReader(const uint8_t *buf) {
    Reset(buf);
  }

  LittlePackedIntsReader(){}

  void Reset(const uint8_t *buf) {
    buf_ = buf; 

    DLOG_IF(FATAL, buf_[0] != PACK_FIRST_BYTE) 
      << "Magic number for pack is wrong: " << buf_[0];

    DLOG_IF(FATAL, buf_[1] > 64) 
      << "n_bits_per_value_ is larger than 64, which is wrong.";

    n_bits_per_value_ = buf[1];
    is_cache_filled_ = false;
  }

  void DecodeToCache() {
    turbounpack32(buf_ + LittlePackedIntsWriter::HEADER_BYTES, PACK_ITEM_CNT, 
        n_bits_per_value_, cache_);
    is_cache_filled_ = true;
  }

  uint32_t Get(const int index) const {
    DLOG_IF(FATAL, is_cache_filled_ == false)
      << "Cache is not filled yet!";

    return cache_[index]; 
  }
  
  int NumBits() const {
    return n_bits_per_value_;
  }

  int SerializationSize() const {
    return LittlePackedIntsWriter::HEADER_BYTES + 
             NumDataBytesInPack(n_bits_per_value_); 
  }

 private:
  const uint8_t *buf_ = nullptr;
  int n_bits_per_value_ = 0;

  uint32_t cache_[PACK_ITEM_CNT];
  bool is_cache_filled_ = false;
};



class PackedIntsReader {
 public:
  PackedIntsReader(const uint8_t *buf) {
    Reset(buf);
  }

  PackedIntsReader(){}

  void Reset(const uint8_t *buf) {
    buf_ = buf; 
    if (buf_[0] != PACK_FIRST_BYTE)
      LOG(FATAL) << "Magic number for pack is wrong: " << buf_[0];

    n_bits_per_value_ = buf[1];
    if (buf_[1] > 64) 
      LOG(FATAL) << "n_bits_per_value_ is larger than 64, which is wrong.";
  }

  long Get(const int index) const {
    return ExtractBits(buf_ + 2, index * n_bits_per_value_, n_bits_per_value_);     
  }
  
  int NumBits() const {
    return n_bits_per_value_;
  }

  int SerializationSize() const {
    // 2 is for the header, +7 is to do ceiling
    return 2 + ((NumBits() * PackedIntsWriter::PACK_SIZE + 7) / 8); 
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

  int SerializationSize() const {
    return reader_.SerializationSize();
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

  void SkipForward(uint64_t val) {
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
  void Append(uint64_t val) {
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
    buf.Append(std::string(1, VINTS_FIRST_BYTE)); 
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
    // int len = utils::varint_decode_uint32((char *)buf, 0, &magic_);
    // buf += len;
    magic_ = buf[0];
    buf += 1;
    if ((magic_ & 0xFF) != VINTS_FIRST_BYTE) {
      printf("Vints Magic: %x\n", magic_);
      LOG(FATAL) << "Magic number is not right for this VInts";
    }

    int len = utils::varint_decode_uint32((char *)buf, 0, &varint_bytes_);
    buf += len;

    varint_iter_.Reset((char *)buf, 0, varint_bytes_);

    next_index_ = 0;
  }

  int SerializationSize() const {
    return 2 + varint_bytes_; // 2 is for the header
  }

  bool IsEnd() const {
    return varint_iter_.IsEnd();
  }

  uint64_t Pop() {
    next_index_++;
    return varint_iter_.Pop();
  }

  uint64_t Peek() const {
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

  uint64_t Pop() {
    prev_value_ = prev_value_ + raw_iter_.Pop();    
    return prev_value_;
  }

  uint64_t Peek() const {
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
