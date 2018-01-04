#include <iostream>
#include <cassert>
#include <unordered_map>
#include <sstream>
#include <cstdlib>

#define NDEBUG

#include <glog/logging.h>

#include "intersect.h"
#include "ranking.h"
#include "utils.h"
#include "posting_list_vec.h"
#include "qq_mem_uncompressed_engine.h"


utils::ResultRow score_bench(const int &n_terms, const int &n_docs) {
  utils::ResultRow result;
  const int n_repeats = 100000;

  TfIdfStore tfidf_store;

  for (int doc_id = 0; doc_id < n_docs; doc_id++) {
    for (int term_id = 0; term_id < n_terms; term_id++) {
      tfidf_store.SetTf(doc_id, std::to_string(term_id), 3);
    }
  }

  for (int term_id = 0; term_id < n_terms; term_id++) {
    tfidf_store.SetDocCount(std::to_string(term_id), 10);
  }

  DocLengthStore lengths_store;
  for (int doc_id = 0; doc_id < n_docs; doc_id++) {
    lengths_store.AddLength(doc_id, 100);
  }

  auto start = utils::now();
  for (int i = 0; i < n_repeats; i++) {
    auto scores = score_docs(tfidf_store, lengths_store);
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  assert(scores.size() == n_docs);

  result["n_terms"] = std::to_string(n_terms);
  result["n_docs"] = std::to_string(n_docs);
  result["duration"] = std::to_string(dur / n_repeats);

  return result;
}


void score_bench_suite() {
  utils::ResultTable result_table;
  result_table.Append(score_bench(1, 100));
  // result_table.Append(score_bench(1, 10000));
  // result_table.Append(score_bench(1, 100000));
  // result_table.Append(score_bench(1, 1000000));

  // result_table.Append(score_bench(1, 1000000));
  // result_table.Append(score_bench(2, 1000000));
  // result_table.Append(score_bench(4, 1000000));
  // result_table.Append(score_bench(8, 1000000));

  std::cout << result_table.ToStr();
}


utils::ResultRow sorting_bench(const int &n_docs, const int &k) {
  utils::ResultRow result;
  const int n_repeats = 100;

  DocScoreVec scores;
  for (int doc_id = 0; doc_id < n_docs; doc_id++) {
    scores.emplace_back(doc_id, std::rand() / 1000.0);
  }

  auto start = utils::now();
  for (int i = 0; i < n_repeats; i++) {
    std::vector<DocIdType> ret = utils::find_top_k(scores, k);
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  std::cout << "Duration: " << dur << std::endl;
  result["duration"] = std::to_string(dur / n_repeats);
  result["n_docs"] = std::to_string(n_docs);
  result["k"] = std::to_string(k);

  return result;
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  score_bench_suite();
}

