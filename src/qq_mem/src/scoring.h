#ifndef SCORING_H
#define SCORING_H


#include <cmath>
#include <array>

#include "types.h"
#include "utils.h"


double calc_tf(const int &term_freq);
double calc_idf(const int &total_docs, const int &doc_freq);
double calc_field_len_norm(const int &field_length);
double calc_score(const int &term_freq, const int &total_docs, 
    const int &doc_freq, const int &field_length);

// More info about ES similarity calculation
// https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html

inline double calc_es_idf(const int &doc_count, const int &doc_freq) {
  // ElasticSearch: 
  // idf, computed as log(1 + (docCount - docFreq + 0.5) / (docFreq + 0.5)) 
  return log(1 + (doc_count - doc_freq + 0.5) / (doc_freq + 0.5));
}


inline double calc_es_tfnorm(const int &freq, 
    const int &field_length, 
    const double &avg_field_length) {
  // tfNorm, computed as 
  // (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * fieldLength / avgFieldLength))
  
  const double k1 = 1.2;
  const double b = 0.75;
  return (freq * (k1 + 1)) / (freq + k1 * (1 - b + ((b * field_length) / avg_field_length)));

  // return (freq * 2.2) / (freq + 1.2 * (0.25 + (0.75 * field_length / avg_field_length)));
  // return (freq * 2.2) / (freq + 1.2 * 0.40);
}


class Bm25Similarity {
 public:
  Bm25Similarity(const double avg_field_length)
      :avg_field_length_(avg_field_length) {
  }

  static double Idf(const int doc_count, const int doc_freq) {
    // ElasticSearch: 
    // idf, computed as log(1 + (docCount - docFreq + 0.5) / (docFreq + 0.5)) 
    return log(1 + (doc_count - doc_freq + 0.5) / (doc_freq + 0.5));
  }

  double TfNorm(const int freq, const int field_length) {
    return TfNormStatic(freq, field_length, avg_field_length_);
  }

  static double TfNormStatic(const int freq, const int field_length, const double avg_field_length) {
    // tfNorm, computed as 
    // (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * fieldLength / avgFieldLength))
    return (freq * (k1_ + 1)) / (freq + k1_ * (1 - b_ + ((b_ * field_length) / avg_field_length)));
  }
  
 protected:
  static constexpr double k1_ = 1.2;
  static constexpr double b_ = 0.75;
  const double avg_field_length_;
};


// It is faster, but lossy, because of the cache
class Bm25SimilarityLossy: public Bm25Similarity {
 public:
  Bm25SimilarityLossy(const double avg_field_length)
      :Bm25Similarity(avg_field_length) {
    BuildCache();
  }

  // field_length here must be a compressed length
  double TfNorm(const int freq, const int field_length) {
    // (freq * (k1 + 1)) / (freq + k1 * (1 - b + ((b * field_length) / avg_field_length)));
    //                             ----------------- cached ---------------------------      
    return (freq * (k1_ + 1)) / (freq + cache_[field_length]);
  }

 private:
  // cache[compressed lengthX] = result for lengthX
  void BuildCache() {
    for (int i = 0; i < 256; i++) {
      uint32_t field_length = utils::Char4ToUint(i & 0xff);
      cache_[i] = k1_ * (1 - b_ + b_ * field_length / avg_field_length_);
    }
  }

  // cache holds k1 * (1 - b + b * fieldLength / avgFieldLength)
  std::array<qq_float, 256> cache_;
};








#endif
