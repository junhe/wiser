#ifndef SCORING_H
#define SCORING_H


#include <cmath>


double calc_tf(const int &term_freq);
double calc_idf(const int &total_docs, const int &doc_freq);
double calc_field_len_norm(const int &field_length);
double calc_score(const int &term_freq, const int &total_docs, 
    const int &doc_freq, const int &field_length);

double calc_es_idf(const int &doc_count, const int &doc_freq);

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
  return (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * field_length / avg_field_length));
}



#endif
