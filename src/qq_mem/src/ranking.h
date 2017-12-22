#ifndef RANKING_H
#define RANKING_H

double calc_tf(const int &term_freq);
double calc_idf(const int &total_docs, const int &doc_freq);
double calc_field_len_norm(const int &field_length);

#endif
