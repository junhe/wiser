#ifndef RANKING_H
#define RANKING_H

double calc_tf(const int &term_freq);
double calc_idf(const int &total_docs, const int &doc_freq);
double calc_field_len_norm(const int &field_length);
double calc_score(const int &term_freq, const int &total_docs, 
    const int &doc_freq, const int &field_length);

double calc_es_idf(const int &doc_count, const int &doc_freq);
double calc_es_tfnorm(const int &freq, const int &field_length, 
    const double &avg_field_length);



#endif
