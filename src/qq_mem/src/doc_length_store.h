#ifndef DOC_LENGTH_STORE_H
#define DOC_LENGTH_STORE_H

#include <unordered_map>
#include <iostream>

#include "types.h"

class DocLengthStore {
 private:
  typedef std::unordered_map<DocIdType, int> length_store_t;

  length_store_t length_dict_;
  qq_float avg_length_ = 0;

 public:
  void AddLength(const DocIdType &doc_id, const int &length) {
    if (length_dict_.find(doc_id) != length_dict_.end()) {
      throw std::runtime_error("The requested doc id already exists. "
          "We do not support update.");
    }

    avg_length_ = avg_length_ + (length - avg_length_) / (length_dict_.size() + 1);
    length_dict_[doc_id] = length;
  }

  int GetLength(const DocIdType &doc_id) const {
    return length_dict_.at(doc_id);  
  }

  qq_float GetAvgLength() const {
    return avg_length_;
  }

  std::size_t Size() const {
    return length_dict_.size();
  }

  std::string ToStr() const {
    std::string str;
    str += "Average doc length: " + std::to_string(avg_length_) + "\n";
    for (auto it : length_dict_) {
      str += std::to_string(it.first) + ": " + std::to_string(it.second) + "\n";
    }

    return str;
  }
};


#endif
