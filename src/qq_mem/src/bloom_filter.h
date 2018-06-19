#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <unordered_map>

#include <glog/logging.h>

#include "bloom.h"

#include "types.h"
#include "utils.h"
#include "compression.h"


typedef struct bloom Bloom;

// Return:
// -------
// 0 - element was not present and was added
// 1 - element (or a collision) had already been added previously
// -1 - bloom not initialized
inline int AddToBloom(Bloom *blm, const std::string &s) {
  return bloom_add(blm, s.data(), s.size()); 
}



// Return:
// -------
// 0 - element is not present
// 1 - element is present (or false positive due to collision)
// -1 - bloom not initialized
inline int CheckBloom(Bloom *blm, const std::string &s) {
  return bloom_check(blm, s.data(), s.size());
}


inline std::string ExtractBitArray(const Bloom *blm) {
  return std::string((char *)blm->bf, blm->bytes);
}

inline Bloom CreateBloomFixedEntries(const float ratio, 
    const int expected_entries, std::vector<std::string> phrase_ends) 
{
  Bloom bloom;
  bloom_init(&bloom, expected_entries, ratio);

  for (auto &end : phrase_ends) {
    int ret = AddToBloom(&bloom, end);
    LOG_IF(FATAL, ret == -1) << "Failed to add to bloom filter";
  }

  return bloom;
};

inline Bloom CreateBloom(
    const float ratio, std::vector<std::string> phrase_ends) 
{
  return CreateBloomFixedEntries(ratio, phrase_ends.size(), phrase_ends);
};


inline void FreeBloom(Bloom *blm) {
  bloom_free(blm);
}

class BloomFilter {
 public:
  BloomFilter() {}

  BloomFilter(const std::string &bit_array) 
    :bit_array_(bit_array) {}

  BloomFilter(const Bloom *blm) {
    bit_array_ = ExtractBitArray(blm);
  }

  int Check(const std::string &elem, const float &ratio, const int &n_entries) const {
    if (bit_array_.size() == 0)
      return BLM_NOT_PRESENT;

    Bloom blm;
    unsigned char *buf = (unsigned char *) malloc(bit_array_.size());
    memcpy(buf, bit_array_.data(), bit_array_.size());
    bloom_set(&blm, n_entries, ratio, buf);

    int ret = CheckBloom(&blm, elem);

    free(buf);

    return ret;
  }

  bool IsEmpty() const {
    return bit_array_.size() == 0;
  }

  const std::string &BitArray() const {
    return bit_array_;
  }

  std::string Serialize() const {
    VarintBuffer buf;   
    buf.Append(bit_array_.size());
    buf.Append(bit_array_);

    return buf.Data();
  }

  const char *Deserialize(const char *buf) {
    uint32_t array_size;
    int len = utils::varint_decode_uint32((const char *)buf, 0, &array_size);
    buf += len; 

    bit_array_ = std::string(buf, array_size);
    buf += array_size;

    return buf;
  }

 private:
  std::string bit_array_;
};


struct BloomFilterCase {
  BloomFilterCase() {}
  BloomFilterCase(const BloomFilter &b, const DocIdType &id) 
    :blm(b), doc_id(id) {}

  std::string Serialize() const {
    VarintBuffer buf;
    buf.Append(doc_id);
    buf.Append(blm.Serialize());

    return buf.Data();
  }

  void Deserialize(const char *buf) {
    int len = utils::varint_decode_uint32((const char *)buf, 0, 
        (uint32_t *)&doc_id);
    buf += len;
    
    blm.Deserialize(buf);
  }

  bool IsEmpty() const {
    return blm.IsEmpty();
  }

  BloomFilter blm;
  DocIdType doc_id;
};


struct CaseCount {
  int n_empty = 0;
  int n_non_empty = 0;

  void Add(const CaseCount &cc) {
    n_empty += cc.n_empty;
    n_non_empty += cc.n_non_empty;
  }

  std::string ToStr() const {
    std::string s;
    s = "empty: " + std::to_string(n_empty) + "\n" + \
         "non-empty: " + std::to_string(n_non_empty) + "\n";

    return s;
  }
};

class BloomFilterCases {
 public:
  void PushBack(const BloomFilterCase &cas) {
    cases_.push_back(cas);
  }

  CaseCount CountCases() const {
    CaseCount count;

    for (auto &cas : cases_) {
      if (cas.IsEmpty()) {
        count.n_empty++;
      } else {
        count.n_non_empty++;
      }
    }

    return count;
  }

  std::size_t Size() const {
    return cases_.size();
  }

  const BloomFilterCase operator[] (int i) {
    return cases_[i];
  }

  std::string Serialize() const {
    VarintBuffer buf;
    
    buf.Append(cases_.size());
    
    for (auto &cas : cases_) {
      std::string data = cas.Serialize(); 
      buf.Append(data.size());
      buf.Append(data);
    }

    return buf.Data();
  }

  std::vector<BloomFilterCase>::const_iterator Begin() const {
    return cases_.cbegin();
  }

  std::vector<BloomFilterCase>::const_iterator End() const {
    return cases_.cend();
  }

  void Deserialize(const char *buf) {
    cases_.clear();

    int len;
    uint32_t n_cases;

    len = utils::varint_decode_uint32(buf, 0, &n_cases);
    buf += len;

    for (uint32_t i = 0; i < n_cases; i++) {
      uint32_t case_size;

      len = utils::varint_decode_uint32(buf, 0, &case_size);
      buf += len;

      BloomFilterCase cas;
      cas.Deserialize(buf);
      cases_.push_back(cas);

      buf += case_size;
    }
  }

 private:
  std::vector<BloomFilterCase> cases_;
};


class BloomFilterStore {
 public:
  BloomFilterStore(const float ratio, const int expected_entries) 
    :ratio_(ratio), expected_entries_(expected_entries),
     end_cnt_hist_(100000, 0) 
  {
    bit_array_bytes_ = bloom_bytes(expected_entries, ratio);   
    std::cout << "======= Bloom Filter Store ===========\n";
    std::cout << "ratio: " << ratio_ << std::endl;
    std::cout << "n entries: " << expected_entries_ << std::endl;
    std::cout << "n bytes: " << bit_array_bytes_ << std::endl;
  }

  BloomFilterStore() :ratio_(0.001), expected_entries_(5),
     end_cnt_hist_(100000, 0) 
  {
    bit_array_bytes_ = bloom_bytes(expected_entries_, ratio_);   
    std::cout << "ratio: " << ratio_ << std::endl;
    std::cout << "n entries: " << expected_entries_ << std::endl;
    std::cout << "n bytes: " << bit_array_bytes_ << std::endl;
  }

  void Add(DocIdType doc_id, std::vector<std::string> tokens, 
      std::vector<std::string> ends) 
  {
    LOG_IF(FATAL, tokens.size() != ends.size())
      << "number of tokens does not match number of ends";
    
    for (std::size_t i = 0 ; i < tokens.size(); i++) {
      std::string token = tokens[i];
      std::vector<std::string> end_list = utils::explode(ends[i], ' ');

      if (end_list.size() >= end_cnt_hist_.size()) {
        std::cout << "one out of bound" << std::endl;
      } else {
        end_cnt_hist_[end_list.size()]++;
      }

      if (end_list.size() > 0) {
        Bloom blm = CreateBloomFixedEntries(ratio_, expected_entries_, end_list);
        LOG_IF(FATAL, bit_array_bytes_ != blm.bytes) 
          << "Bytes do not match!";

        BloomFilter blm_filter(&blm);
        FreeBloom(&blm);

        BloomFilterCase blm_case(blm_filter, doc_id);
        filter_map_[token].PushBack(blm_case);
      } else {
        BloomFilter blm_filter("");
        BloomFilterCase blm_case(blm_filter, doc_id);
        filter_map_[token].PushBack(blm_case);
      }
    }

    n++;
    if (n % 10000 == 0) {
      std::vector<std::string> lines;
      for (std::size_t i = 0; i < 100; i++) {
        std::string line = std::to_string(i) + ":" + std::to_string(end_cnt_hist_[i]);
        lines.push_back(line);
      }
      std::cout 
        << "phrase end histogram has been written to /tmp/ends_hist.txt" 
        << std::endl;
      utils::AppendLines("/tmp/ends_hist.txt", lines);
    }
  }

  const BloomFilterCases &Lookup(const Term &term) {
    return filter_map_[term];
  }

  const void Serialize(const std::string &path) const {
    std::ofstream out(path, std::ios::binary);
    
    std::string ratio = utils::SerializeFloat(ratio_);
    out.write(ratio.data(), ratio.size());

    out.write((const char*)&expected_entries_, sizeof(expected_entries_));

    for (auto &it : filter_map_) {
      VarintBuffer buf;
      buf.Append(it.first.size());
      buf.Append(it.first);

      std::string cases_data = it.second.Serialize();
      buf.Append(cases_data.size());
      buf.Append(cases_data);

      out.write(buf.DataPointer()->data(), buf.Size());
    }

    out.close();
  }

  void Deserialize(const std::string &path) {
    filter_map_.clear();

    if (utils::GetFileSize(path) == 0)
      return;

    utils::FileMap file_map;
    file_map.Open(path);
  
    const char *buf = file_map.Addr();
    const char *const end = buf + file_map.Length();

    ratio_ = utils::DeserializeFloat(buf);
    buf += sizeof(float);

    expected_entries_ = *((int *)buf);
    buf += sizeof(int);

    bit_array_bytes_ = bloom_bytes(expected_entries_, ratio_);   

    CaseCount cnt;
    int n = 0;
    while (buf < end) {
      buf = DeserializeEntry(buf, &cnt);
      n++;

      if (n % 100000 == 0)
        std::cout << cnt.ToStr();
    }

    std::cout << "Bloom filter stats:" << std::endl;
    std::cout << cnt.ToStr();
  }

  const char *DeserializeEntry(const char *buf, CaseCount *cnt) {
    uint32_t n;
    int len;

    len = utils::varint_decode_uint32((const char *)buf, 0, &n);
    buf += len;

    std::string term(buf, n);
    buf += n;

    len = utils::varint_decode_uint32((const char *)buf, 0, &n);
    buf += len;

    BloomFilterCases cases;
    cases.Deserialize(buf);
    buf += n;

    filter_map_[term] = cases;

    cnt->Add(cases.CountCases());

    return buf;
  }

  float Ratio() const {
    return ratio_;
  }

  int ExpectedEntries() const {
    return expected_entries_;
  }

  int BitArrayBytes() const {
    return bit_array_bytes_;
  }

 private:
  std::unordered_map<std::string, BloomFilterCases> filter_map_;
  float ratio_;
  int expected_entries_;
  int bit_array_bytes_;
  std::vector<int> end_cnt_hist_;
  int n = 0;
};


#endif

