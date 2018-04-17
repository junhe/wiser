#ifndef VACUUM_ENGINE_H
#define VACUUM_ENGINE_H

#include <iostream>
#include <string>

#include "utils.h"
#include "flash_iterators.h"


class TermIndex {
 public:
  typedef std::unordered_map<Term, off_t> LocalMap;
  typedef LocalMap::const_iterator ConstIterator;

  void Load(const std::string &path) {
    int fd;
    char *addr;
    size_t file_length;
    utils::MapFile(path, &addr, &fd, &file_length);

    const char *end = addr + file_length;
    const char *buf = addr;

    while (buf < end) {
      buf = LoadEntry(buf);
    }

    utils::UnmapFile(addr, fd, file_length);

    for (auto &it : index_) {
      LOG(INFO) << ".tip: " << it.first << ": " << it.second << std::endl;
    }
  }

  const char *LoadEntry(const char *buf) {
    uint32_t term_size = *((uint32_t *)buf);

    buf += sizeof(uint32_t);
    std::string term(buf , term_size);

    buf += term_size;
    off_t offset = *((off_t *)buf);

    index_[term] = offset;

    return buf + sizeof(off_t);
  }

  int NumTerms () const {
    return index_.size();
  }

  ConstIterator Find(const Term &term) {
    ConstIterator it = index_.find(term);
    return it;
  }

  ConstIterator CEnd() {
    return index_.cend();
  }

 private:
  LocalMap index_;
};


class VacuumInvertedIndex {
 public:
  VacuumInvertedIndex(
      const std::string term_index_path, 
      const std::string inverted_index_path)
    : file_map_(inverted_index_path)
  {
    term_index_.Load(term_index_path);
    file_data_ = (uint8_t *)file_map_.Addr();
    std::cout << "file_map_.Addr(): " << (void *)file_data_ << std::endl;
  }

  off_t FindPostingListOffset(const Term term) {
    TermIndex::ConstIterator it = term_index_.Find(term);
    if (it == term_index_.CEnd()) {
      return -1;
    } else {
      return it->second;
    }
  }

  std::vector<VacuumPostingListIterator> FindIteratorsSolid(const TermList &terms) {
    std::vector<VacuumPostingListIterator> iterators;

    for (auto &term : terms) {
      off_t offset = FindPostingListOffset(term); 
      if (offset != -1) {
        iterators.emplace_back(file_data_, offset);
      }
    }
    return iterators;
  }

  int NumTerms() const {
    return term_index_.NumTerms();
  }


 private:
  TermIndex term_index_;  
  utils::FileMap file_map_;
  const uint8_t *file_data_; // = file_map_.Addr(), put it here for convenience
};

#endif

