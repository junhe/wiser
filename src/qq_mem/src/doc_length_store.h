#ifndef DOC_LENGTH_STORE_H
#define DOC_LENGTH_STORE_H

#include <unordered_map>
#include <iostream>
#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "types.h"
#include "utils.h"


class DocLengthStore {
 public:
  void AddLength(const DocIdType &doc_id, const int &length) {
    if ((std::size_t) doc_id >= vec_store_.size()) {
      vec_store_.resize(doc_id + 1, -1);
      vec_char_store_.resize(doc_id + 1, 0);
    }
    avg_length_ = avg_length_ + (length - avg_length_) / (doc_cnt_ + 1);

    vec_store_[doc_id] = length;
    vec_char_store_[doc_id] = utils::UintToChar4(length);
    doc_cnt_++;
  }

  int GetLength(const DocIdType &doc_id) const noexcept {
    return vec_store_[doc_id];
  }

  char GetChar4Length(const DocIdType &doc_id) const noexcept {
    return vec_char_store_[doc_id];
  }

  const qq_float &GetAvgLength() const noexcept {
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

    for (int doc_id = 0; doc_id < count; doc_id++) {
      ofile.write((char *)&doc_id, sizeof(doc_id));
      ofile.write((char *)&vec_store_[doc_id], sizeof(vec_store_[doc_id]));
    }

    ofile.close();	
  }

  void Deserialize(std::string path) {
    int fd;
    char *addr;
    size_t file_length;

    avg_length_ = 0;

    utils::MapFile(path, &addr, &fd, &file_length);

    int count = *((int *)addr);
    
    int width = sizeof(int);
    const char *base = addr + width;
    for (int i = 0; i < count; i++) {
      int id = *((int *) (base + width * (2 * i)));
      int length = *((int *) (base + width * (2 * i + 1)));
      if (length != -1) {
        AddLength(id, length);
      }
    }

    utils::UnmapFile(addr, fd, file_length);
  }

 private:
  std::vector<int> vec_store_;
  std::vector<char> vec_char_store_;
  qq_float avg_length_ = 0;
  int doc_cnt_ = 0;
};

// the lengths are compressed and stored in char
class DocLengthCharStore {
 public:
  void AddLength(const DocIdType &doc_id, const int &length) {
    if ((std::size_t)doc_id >= vec_char_store_.size()) {
      vec_char_store_.resize(doc_id + 1, 0);
    }
    avg_length_ = avg_length_ + (length - avg_length_) / (doc_cnt_ + 1);

    vec_char_store_[doc_id] = utils::UintToChar4(length);
    doc_cnt_++;
  }

  char GetLength(const DocIdType &doc_id) const noexcept {
    return vec_char_store_[doc_id];
  }

  int GetDecodedLength(const DocIdType &doc_id) const noexcept {
    return utils::Char4ToUint(vec_char_store_[doc_id]);
  }

  const qq_float &GetAvgLength() const noexcept {
    return avg_length_;
  }

  int Size() const {
    return doc_cnt_;
  }

  std::string ToStr() const {
    std::string str;
    str += "Average doc length: " + std::to_string(avg_length_) + "\n";
    for (int doc_id = 0; doc_id < doc_cnt_; doc_id++) {
      str += std::to_string(doc_id) + ": " + std::to_string(vec_char_store_[doc_id]) + "\n";
    }

    return str;
  }

  void Serialize(std::string path) const {
    int fd = open(path.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0666);
    int ret;
    if (fd == -1)
      LOG(FATAL) << "cannot open file";

    int count = vec_char_store_.size();
    ret = write(fd, &count, sizeof(int));
    DLOG_IF(FATAL, ret != sizeof(int)) << "Write error";
    ret = write(fd, &avg_length_, sizeof(qq_float));
    DLOG_IF(FATAL, ret != sizeof(qq_float)) << "Write error";

    for (int doc_id = 0; doc_id < count; doc_id++) {
      ret = write(fd, (char *)&doc_id, sizeof(int));
      DLOG_IF(FATAL, ret != sizeof(int)) << "Write error";
      ret = write(fd, (char *)&vec_char_store_[doc_id], sizeof(char));
      DLOG_IF(FATAL, ret != sizeof(char)) << "Write error";
    }

    fsync(fd);
    close(fd);
  }

  void Deserialize(std::string path) {
    doc_cnt_ = 0;

    utils::FileMap file_map;
    file_map.Open(path);

    const char *p = file_map.Addr();
    int count = *(int *)p;
    p += sizeof(int);

    avg_length_ = *(qq_float *)p;
    p += sizeof(qq_float);

    int doc_id;
    char length;
    
    for (int i = 0; i < count; i++) {
      doc_id = *((int *) p);
      p += sizeof(doc_id);

      length = *((char *) p);
      p += sizeof(length);

      AddCharLength(doc_id, length);
    }

    file_map.Close();
  }

  void Show() {
    for (std::size_t i = 0; i < vec_char_store_.size() && i < 100; i++) {
      std::cout << "docid: " << i << " len: " << GetDecodedLength(i) << "; ";
    }
    std::cout << std::endl;
  }

 private:
  void AddCharLength(const DocIdType doc_id, const char length) {
    if ((std::size_t) doc_id >= vec_char_store_.size()) {
      vec_char_store_.resize(doc_id + 1, 0);
    }

    vec_char_store_[doc_id] = length;
    doc_cnt_++;
  }

  std::vector<char> vec_char_store_;
  qq_float avg_length_ = 0;
  int doc_cnt_ = 0;
};


#endif
