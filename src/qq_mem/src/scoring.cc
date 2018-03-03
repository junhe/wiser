#include "scoring.h"



// the input term_freq is the number of times the term appears
// in a particular document
double calc_tf(const int &term_freq) {
  return sqrt(term_freq);
}

double calc_idf(const int &total_docs, const int &doc_freq) {
  // the log base used by ES is `e`.
  return 1.0 + log( total_docs / (doc_freq + 1.0));
}

double calc_field_len_norm(const int &field_length) {
  return 1.0 / sqrt(field_length);
}

// This is the score of a doc for a particular term
double calc_score(const int &term_freq, const int &total_docs, 
    const int &doc_freq, const int &field_length) {
  return calc_tf(term_freq) * calc_idf(total_docs, doc_freq) * 
    calc_field_len_norm(field_length);
}


