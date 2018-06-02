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


const int BLM_NOT_PRESENT = 0;
const int BLM_MAY_PRESENT = 1;
const int BLM_NOT_INITIALIZED = -1;


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


inline Bloom CreateBloom(
    const float ratio, std::vector<std::string> phrase_ends) 
{
  Bloom bloom;
  const int expected_entries = phrase_ends.size();
  bloom_init(&bloom, expected_entries, ratio);

  for (auto &end : phrase_ends) {
    int ret = AddToBloom(&bloom, end);
    assert(ret == 0);
  }

  return bloom;
};


inline void FreeBloom(Bloom *blm) {
  bloom_free(blm);
}


class BloomFilter {
 public:
  BloomFilter() {}
  BloomFilter(const Bloom *blm, const float ratio) {
    n_entries_ = blm->entries;
    ratio_ = ratio;
    bit_array_ = ExtractBitArray(blm);
  }

  BloomFilter(const int n_entries, const float ratio, const std::string &bit_array) 
    :n_entries_(n_entries), ratio_(ratio), bit_array_(bit_array)
  {}

  int Check(const std::string &elem) const {
    Bloom blm;
    unsigned char *buf = (unsigned char *) malloc(bit_array_.size());
    memcpy(buf, bit_array_.data(), bit_array_.size());
    bloom_set(&blm, n_entries_, ratio_, buf);

    int ret = CheckBloom(&blm, elem);

    free(buf);

    return ret;
  }

  const std::string &BitArray() const {
    return bit_array_;
  }

  std::string Serialize() const {
    VarintBuffer buf;   
    buf.Append(n_entries_);
    buf.Append(utils::SerializeFloat(ratio_));
    buf.Append(bit_array_.size());
    buf.Append(bit_array_);

    return buf.Data();
  }

  const char *Deserialize(const char *buf) {
    int len = utils::varint_decode_uint32((const char *)buf, 0, &n_entries_);
    buf += len;

    ratio_ = utils::DeserializeFloat(buf);
    buf += sizeof(ratio_);
    
    uint32_t array_size;
    len = utils::varint_decode_uint32((const char *)buf, 0, &array_size);
    buf += len; 

    bit_array_ = std::string(buf, array_size);
    buf += array_size;

    return buf;
  }

 private:
  uint32_t n_entries_;
  float ratio_;
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

  BloomFilter blm;
  DocIdType doc_id;
};



class BloomFilterCases {
 public:
  void PushBack(const BloomFilterCase &cas) {
    cases_.push_back(cas);
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
  BloomFilterStore(float ratio) :ratio_(ratio) {}
  BloomFilterStore() :ratio_(0.001) {}

  void Add(DocIdType doc_id, std::vector<std::string> tokens, 
      std::vector<std::string> ends) 
  {
    DLOG_IF(FATAL, tokens.size() != ends.size())
      << "number of tokens does not match number of ends";
    
    for (std::size_t i = 0 ; i < tokens.size(); i++) {
      std::string token = tokens[i];
      std::vector<std::string> end_list = utils::explode(ends[i], ' ');

      Bloom blm = CreateBloom(ratio_, end_list);
      BloomFilter blm_filter(&blm, ratio_);
      FreeBloom(&blm);

      BloomFilterCase blm_case(blm_filter, doc_id);
      filter_map_[token].PushBack(blm_case);
    }
  }

  const BloomFilterCases Lookup(const Term &term) {
    return filter_map_[term];
  }

  const void Serialize(const std::string &path) {
    std::ofstream out(path, std::ios::binary);
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

    utils::FileMap file_map;
    file_map.Open(path);
  
    const char *buf = file_map.Addr();
    const char *const end = buf + file_map.Length();

    while (buf < end) {
      buf = DeserializeEntry(buf);
    }
  }

  const char *DeserializeEntry(const char *buf) {
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
    return buf;
  }

 private:
  std::unordered_map<std::string, BloomFilterCases> filter_map_;
  float ratio_;
};


