#ifndef DOC_LENGTH_STORE_H
#define DOC_LENGTH_STORE_H

#include <unordered_map>
#include <iostream>
#include <fstream>

#include "types.h"
#include "utils.h"

class DocLengthStore {
 private:
  typedef std::unordered_map<DocIdType, int> length_store_t;

  std::vector<int> vec_store_;
  length_store_t length_dict_;
  qq_float avg_length_ = 0;
  int doc_cnt_ = 0;

 public:
  DocLengthStore() {
    vec_store_.reserve(6000000);
  }

  void AddLength(const DocIdType &doc_id, const int &length) {
    throw std::runtime_error("should not call me");
    if (length_dict_.find(doc_id) != length_dict_.end()) {
      throw std::runtime_error("The requested doc id already exists. "
          "We do not support update.");
    }

    avg_length_ = avg_length_ + (length - avg_length_) / (length_dict_.size() + 1);
    length_dict_[doc_id] = length;
  }

  void AddLength2(const DocIdType &doc_id, const int &length) {
    if (doc_id >= vec_store_.size()) {
      vec_store_.resize(doc_id + 1);
    }
    avg_length_ = avg_length_ + (length - avg_length_) / (doc_cnt_ + 1);

    vec_store_[doc_id] = length;
    doc_cnt_++;
  }

  int GetLength(const DocIdType &doc_id) const {
    // return 301;
    // return length_dict_.at(doc_id);  
    return vec_store_[doc_id];
  }

  const qq_float &GetAvgLength() const {
    return avg_length_;
  }

  int Size() const {
    // return length_dict_.size();
    return doc_cnt_;
  }

  std::string ToStr() const {
    std::string str;
    str += "Average doc length: " + std::to_string(avg_length_) + "\n";
    for (auto it : length_dict_) {
      str += std::to_string(it.first) + ": " + std::to_string(it.second) + "\n";
    }

    return str;
  }

  void Serialize(std::string path) const {
    std::ofstream ofile(path, std::ios::binary);

    int count = length_dict_.size();
    ofile.write((char *)&count, sizeof(count));

    for (auto it = length_dict_.cbegin(); it != length_dict_.cend(); it++) {
      ofile.write((char *)&it->first, sizeof(it->first));
      ofile.write((char *)&it->second, sizeof(it->second));
    }

    ofile.close();	
  }

  void Deserialize(std::string path) {
    int fd;
    int len;
    char *addr;
    size_t file_length;
    uint32_t var;
    off_t offset = 0;

    avg_length_ = 0;

    utils::MapFile(path, &addr, &fd, &file_length);

    int count = *((int *)addr);
    std::cout << "count: " << count << std::endl;
    
    int width = sizeof(int);
    const char *base = addr + width;
    for (int i = 0; i < count; i++) {
      int id = *((int *) (base + width * (2 * i)));
      int length = *((int *) (base + width * (2 * i + 1)));
      AddLength2(id, length);
    }

    utils::UnmapFile(addr, fd, file_length);
  }
};


class FastDocLengthStore {
 private:
  std::vector<int> vec_store_;
  qq_float avg_length_ = 0;
  int doc_cnt_ = 0;

 public:
  FastDocLengthStore() {
    vec_store_.reserve(6000000);
  }

  void AddLength(const DocIdType &doc_id, const int &length) {
    if (doc_id >= vec_store_.size()) {
      vec_store_.resize(doc_id + 1);
    }
    avg_length_ = avg_length_ + (length - avg_length_) / (doc_cnt_ + 1);

    vec_store_[doc_id] = length;
    doc_cnt_++;
  }

  int GetLength(const DocIdType &doc_id) const {
    return vec_store_[doc_id];
  }

  const qq_float &GetAvgLength() const {
    return avg_length_;
  }

  int Size() const {
    return doc_cnt_;
  }

  std::string ToStr() const {
    std::string str;
    str += "Average doc length: " + std::to_string(avg_length_) + "\n";
    for (int doc_id = 0; doc_id < doc_cnt_; doc_id++) {
      str += std::to_string(doc_id) + ": " + std::to_string(vec_store_[doc_id]) + "\n";
    }

    return str;
  }

  void Serialize(std::string path) const {
    std::ofstream ofile(path, std::ios::binary);

    int count = vec_store_.size();
    ofile.write((char *)&count, sizeof(count));

    for (int doc_id = 0; doc_id < doc_cnt_; doc_id++) {
      ofile.write((char *)&doc_id, sizeof(doc_id));
      ofile.write((char *)&vec_store_[doc_id], sizeof(vec_store_[doc_id]));
    }

    ofile.close();	
  }

  void Deserialize(std::string path) {
    int fd;
    int len;
    char *addr;
    size_t file_length;
    uint32_t var;
    off_t offset = 0;

    avg_length_ = 0;

    utils::MapFile(path, &addr, &fd, &file_length);

    int count = *((int *)addr);
    std::cout << "count: " << count << std::endl;
    
    int width = sizeof(int);
    const char *base = addr + width;
    for (int i = 0; i < count; i++) {
      int id = *((int *) (base + width * (2 * i)));
      int length = *((int *) (base + width * (2 * i + 1)));
      AddLength(id, length);
    }

    utils::UnmapFile(addr, fd, file_length);
  }
};



#endif
