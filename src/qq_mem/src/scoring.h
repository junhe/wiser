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
  Bm25Similarity()
      :avg_field_length_(1) {}

  Bm25Similarity(const double avg_field_length)
      :avg_field_length_(avg_field_length) {
    BuildCache();
  }

  void Reset(const double avg_field_length) {
    avg_field_length_ = avg_field_length;
    BuildCache();
  }

  static double Idf(const int doc_count, const int doc_freq) {
    // ElasticSearch: 
    // idf, computed as log(1 + (docCount - docFreq + 0.5) / (docFreq + 0.5)) 
    return log(1 + (doc_count - doc_freq + 0.5) / (doc_freq + 0.5));
  }

  // field_length here must be a compressed length
  double TfNormLossy(const int freq, const char field_length) const noexcept {
    // (freq * (k1 + 1)) / (freq + k1 * (1 - b + ((b * field_length) / avg_field_length)));
    //                             ----------------- cached ---------------------------      
    return (freq * (k1_ + 1)) / (freq + cache_[field_length]);
  }

  double TfNorm(const int freq, const int field_length) const noexcept {
    return TfNormStatic(freq, field_length, avg_field_length_);
  }

  static double TfNormStatic(const int freq, 
                             const int field_length, 
                             const double avg_field_length) noexcept {
    // tfNorm, computed as 
    // (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * fieldLength / avgFieldLength))
    return (freq * (k1_ + 1)) / (freq + k1_ * (1 - b_ + ((b_ * field_length) / avg_field_length)));
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
  static constexpr double k1_ = 1.2;
  static constexpr double b_ = 0.75;
  double avg_field_length_;
};


template<typename IteratorT>
inline qq_float CalcDocScoreNonLossy(
    std::vector<IteratorT> &pl_iterators,
    const std::vector<qq_float> &idfs_of_terms,
    const int &length_of_this_doc, 
    const Bm25Similarity &similarity) noexcept
{
  qq_float final_doc_score = 0;

  for (int list_i = 0; list_i < pl_iterators.size(); list_i++) {
    const int cur_term_freq = pl_iterators[list_i].TermFreq(); 

    qq_float idf = idfs_of_terms[list_i];
    qq_float tfnorm = similarity.TfNorm(cur_term_freq, length_of_this_doc);

    qq_float term_doc_score = idf * tfnorm;

    final_doc_score += term_doc_score;
  }

  return final_doc_score;
}


template<typename IteratorT>
inline qq_float CalcDocScoreLossy(
    std::vector<IteratorT> &pl_iterators,
    const std::vector<qq_float> &idfs_of_terms,
    const int &length_of_this_doc, 
    const Bm25Similarity &similarity) noexcept
{
  qq_float final_doc_score = 0;

  for (std::size_t list_i = 0; list_i < pl_iterators.size(); list_i++) {
    const int cur_term_freq = pl_iterators[list_i].TermFreq(); 

    qq_float idf = idfs_of_terms[list_i];
    qq_float tfnorm = similarity.TfNormLossy(cur_term_freq, length_of_this_doc);

    qq_float term_doc_score = idf * tfnorm;

    final_doc_score += term_doc_score;
  }

  return final_doc_score;
}


template<typename IteratorT>
inline qq_float CalcDocScore(
    std::vector<IteratorT> &pl_iterators,
    const std::vector<qq_float> &idfs_of_terms,
    const int &length_of_this_doc, 
    const Bm25Similarity &similarity) noexcept
{
  return CalcDocScoreLossy<IteratorT>(pl_iterators, idfs_of_terms,
      length_of_this_doc, similarity);

  // return CalcDocScoreNonLossy<IteratorT>(pl_iterators, idfs_of_terms,
      // length_of_this_doc, similarity);
}


// IteratorT.TermFreq() must exist
template<typename IteratorT>
inline qq_float CalcDocScoreForOneQuery(
    std::vector<IteratorT> &pl_iterators,
    const std::vector<qq_float> &idfs_of_terms,
    const int &n_total_docs_in_index, 
    const qq_float &avg_doc_length_in_index,
    const int &length_of_this_doc) 
{
  qq_float final_doc_score = 0;

  for (int list_i = 0; list_i < pl_iterators.size(); list_i++) {
    // calc score of a term in one loop

    const int cur_term_freq = pl_iterators[list_i].TermFreq(); 

    qq_float idf = idfs_of_terms[list_i];
    qq_float tfnorm = calc_es_tfnorm(cur_term_freq, length_of_this_doc, 
        avg_doc_length_in_index);

    // ES 6.1 uses tfnorm * idf 
    qq_float term_doc_score = idf * tfnorm;

    final_doc_score += term_doc_score;
  }

  return final_doc_score;
}




#endif
