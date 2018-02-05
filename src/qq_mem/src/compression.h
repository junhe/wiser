#ifndef COMPRESSION_H
#define COMPRESSION_H

#include "utils.h"

class VarintBuffer {
 public:
  void Append(uint32_t val) {
    int len = utils::varint_expand_and_encode(val, &data_, end_);
    end_ += len;
  }

  void Prepend(uint32_t val) {
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

  int Size() const {
    return end_;
  }

  int End() const {
    return end_;
  }

  // CAUTION! It returns a copy!
  std::string Data() const {
    return std::string(data_, 0, end_);
  }

  const std::string *DataPointer() const {
    return &data_;
  }

 private:
  std::string data_;
  int end_ = 0;
};


class VarintIterator: public VarintIteratorService {
 public:
  VarintIterator(const std::string *data, const int start_offset, const int count)
    :data_(data), start_offset_(start_offset), cur_offset_(start_offset), 
     count_(count) {}

  VarintIterator(const VarintBuffer &varint_buf, int count)
    :data_(varint_buf.DataPointer()), start_offset_(0), 
     cur_offset_(0), count_(count) {}

  bool IsEnd() const {
    return index_ == count_;
  }

  // Must check if it is the end before Pop() is called!
  uint32_t Pop() {
    int len;
    uint32_t n;

    len = utils::varint_decode(*data_, cur_offset_, &n);
    cur_offset_ += len;
    index_++;

    return n;
  }

 private:
  const std::string *data_;
  int cur_offset_; 
  int index_ = 0;
  const int start_offset_;
  const int count_;
};

class VarintIteratorEndBound: public VarintIteratorService {
 public:
  VarintIteratorEndBound(const std::string *data, const int start_offset, 
                 const int end_offset)
    :data_(data), start_offset_(start_offset), cur_offset_(start_offset), 
     end_offset_(end_offset) {}

  VarintIteratorEndBound(const VarintBuffer &varint_buf, const int end_offset)
    :data_(varint_buf.DataPointer()), start_offset_(0), 
     cur_offset_(0), end_offset_(end_offset) {}

  VarintIteratorEndBound(const VarintBuffer &varint_buf)
    :data_(varint_buf.DataPointer()), start_offset_(0), 
     cur_offset_(0), end_offset_(varint_buf.Size()) {}

  bool IsEnd() const {
    return cur_offset_ >= end_offset_;
  }

  // Must check if it is the end before Pop() is called!
  uint32_t Pop() {
    int len;
    uint32_t n;

    len = utils::varint_decode(*data_, cur_offset_, &n);
    cur_offset_ += len;

    return n;
  }

 private:
  const std::string *data_;
  int cur_offset_; 
  const int start_offset_;
  const int end_offset_;
};




#endif
