#ifndef TERM_INDEX_H
#define TERM_INDEX_H

#include "tsl/htrie_hash.h"
#include "tsl/htrie_map.h"

class TermIndexResult {
 public:
  TermIndexResult() :is_empty_(true) {}
  TermIndexResult(std::string key, off_t value, bool is_empty)
    :key_(key), is_empty_(is_empty) {
    DecodePrefetchZoneAndOffset(
        value, &n_pages_of_prefetch_zone_, &posting_list_offset_);
  }

  const std::string &Key() const {
    return key_;
  }

  off_t GetPostingListOffset() const {
    return posting_list_offset_;
  }

  uint32_t GetNumPagesInPrefetchZone() const {
    return n_pages_of_prefetch_zone_;
  }

  bool IsEmpty() const {
    return is_empty_;
  }

 private:
  std::string key_;

  uint32_t n_pages_of_prefetch_zone_;
  off_t posting_list_offset_;

  bool is_empty_;
};


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

    int cnt = 0;
    while (buf < end) {
      buf = LoadEntry(buf);
      cnt++;
      if (cnt % 1000000 == 0) {
        std::cout << "Term index entries loaded: " 
          << utils::format_with_commas<int>(cnt) << std::endl;
      }
    }

    utils::UnmapFile(addr, fd, file_length);
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

  TermIndexResult Find(const Term &term) {
    ConstIterator it = index_.find(term);
    if (it == index_.cend()) {
      return TermIndexResult("", -1, true);
    } else {
      return TermIndexResult(it->first, it->second, false);
    }
  }

 private:
  LocalMap index_;
};


class TermTrieIndex {
 public:
  typedef tsl::htrie_map<char, int64_t> LocalTrieMap; 
  typedef LocalTrieMap::const_iterator ConstTrieIterator;

  void Load(const std::string &path) {
    int fd;
    char *addr;
    size_t file_length;
    utils::MapFile(path, &addr, &fd, &file_length);

    const char *end = addr + file_length;
    const char *buf = addr;

    int cnt = 0;
    while (buf < end) {
      buf = LoadEntry(buf);
      cnt++;
      if (cnt % 1000000 == 0) {
        std::cout << "Term index entries loaded: " 
          << utils::format_with_commas<int>(cnt) << std::endl;
      }
    }

    utils::UnmapFile(addr, fd, file_length);
  }

  int NumTerms () const {
    return trieindex_.size();
  }

  void Add(const std::string &key, const uint64_t &value) {
    trieindex_[key.c_str()] = value;
  }

  TermIndexResult Find(const Term &term) {
    ConstTrieIterator it = trieindex_.find(term.c_str());

    if (it == trieindex_.end()) {
      return TermIndexResult("", -1, true);
    } else {
      return TermIndexResult(it.key(), it.value(), false);
    }
  }

 private:
  const char *LoadEntry(const char *buf) {
    uint32_t term_size = *((uint32_t *)buf);

    buf += sizeof(uint32_t);
    std::string term(buf , term_size);

    buf += term_size;
    off_t offset = *((off_t *)buf);

    Add(term, offset);

    return buf + sizeof(off_t);
  }


  LocalTrieMap trieindex_;
};



#endif
