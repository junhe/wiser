#ifndef COMPRESSION_H
#define COMPRESSION_H

#include "utils.h"

class VarintBuffer {
 public:
  void Append(uint64_t val) {
    int len = utils::varint_expand_and_encode(val, &data_, end_);
    end_ += len;
  }

  void Prepend(uint64_t val) {
    std::string tmp_buf;
    int len = utils::varint_expand_and_encode(val, &tmp_buf, 0);
    data_.insert(0, tmp_buf, 0, len);
    end_ += len;
  }

  void Append(const std::string &buf) {
    data_.insert(end_, buf);
    end_ += buf.size();
  }

  void Prepend(const std::string &buf) {
    data_.insert(0, buf);
    end_ += buf.size();
  }

  off_t Size() const {
    return end_;
  }

  off_t End() const {
    return end_;
  }

  // CAUTION! It returns a copy!
  std::string Data() const {
    return std::string(data_, 0, end_);
  }

  const std::string *DataPointer() const {
    return &data_;
  }

  // Format
  // end_ | data_
  std::string Serialize() const {
    VarintBuffer out;
    out.Append(end_);
    out.Append(Data());
    
    return out.Data();
  }

  off_t Deserialize(const std::string &buf, const off_t offset) {
    int len;
    uint64_t end;

    len = utils::varint_decode_64bit(buf.data(), offset, &end);
    end_ = end;
    
    data_ = std::string(buf, offset + len, offset + len + end_);
    
    return len + end_;
  }

 private:
  std::string data_;
  off_t end_ = 0;
};


class VarintIterator: public PopIteratorService {
 public:
  VarintIterator() {}

  VarintIterator(const std::string *data, const int start_offset, const int count)
    :data_(data->data()), start_offset_(start_offset), cur_offset_(start_offset), 
     count_(count) {}

  VarintIterator(const char *data, const int start_offset, const int count)
    :data_(data), start_offset_(start_offset), cur_offset_(start_offset), 
     count_(count) {}

  VarintIterator(const VarintBuffer &varint_buf, int count)
    :data_(varint_buf.DataPointer()->data()), start_offset_(0), 
     cur_offset_(0), count_(count) {}

  VarintIterator(const VarintIterator &rhs)
    :data_(rhs.data_), cur_offset_(rhs.cur_offset_), index_(rhs.index_),
     start_offset_(rhs.start_offset_), count_(rhs.count_)
  {}

  VarintIterator& operator=(const VarintIterator &rhs) {
    data_ = rhs.data_;
    cur_offset_ = rhs.cur_offset_; 
    index_ = rhs.index_;
    start_offset_ = rhs.start_offset_; 
    count_ = rhs.count_;

    return *this;
  }

  bool IsEnd() const {
    return index_ == count_;
  }

  // Must check if it is the end before Pop() is called!
  uint64_t Pop() {
    int len;
    uint64_t n;

    len = utils::varint_decode_64bit(data_, cur_offset_, &n);
    cur_offset_ += len;
    index_++;

    return n;
  }

 private:
  const char *data_;
  int cur_offset_; 
  int index_ = 0;
  int start_offset_;
  int count_;
};


class VarintIteratorEndBound: public PopIteratorService {
 public:
  VarintIteratorEndBound() 
    :data_(nullptr), cur_offset_(0), start_offset_(0), end_offset_(0) {}

  VarintIteratorEndBound(const std::string *data, const int start_offset, 
                 const int end_offset) {
    Reset(data->data(), start_offset, end_offset);
  }

  VarintIteratorEndBound(const VarintBuffer &varint_buf, const int end_offset) {
    Reset(varint_buf.DataPointer()->data(), 0, end_offset);
  }

  VarintIteratorEndBound(const VarintBuffer &varint_buf) {
    Reset(varint_buf.DataPointer()->data(), 0, varint_buf.Size());
  }

  void Reset(const char *data, const int start_offset, const int end_offset) {
    data_ = data;
    start_offset_ = start_offset;
    cur_offset_ = start_offset;
    end_offset_ = end_offset;
  }

  bool IsEnd() const {
    return cur_offset_ >= end_offset_;
  }

  int NumBytes() const {
    return end_offset_ - start_offset_;
  }

  // Must check if it is the end before Pop() is called!
  uint64_t Pop() {
    int len;
    uint64_t n;

    len = utils::varint_decode_64bit(data_, cur_offset_, &n);
    cur_offset_ += len;

    return n;
  }

  uint64_t Peek() const {
    uint64_t n;
    utils::varint_decode_64bit(data_, cur_offset_, &n);
    return n;
  }

 private:
  const char *data_;
  int cur_offset_; 
  int start_offset_;
  int end_offset_;
};




#endif
