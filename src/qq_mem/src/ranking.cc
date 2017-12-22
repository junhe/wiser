#include "ranking.h"

#include <cmath>

// the input term_freq is the number of times the term appears
// in a particular document
double calc_tf(const int &term_freq) {
  return sqrt(term_freq);
}

double calc_idf(const int &total_docs, const int &doc_freq) {
  return 1.0 + log10( total_docs / (doc_freq + 1.0));
}

double calc_field_len_norm(const int &field_length) {
  return 1.0 / sqrt(field_length);
}

