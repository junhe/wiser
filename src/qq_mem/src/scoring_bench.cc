#include <iostream>
#include <cassert>
#include <unordered_map>
#include <sstream>
#include <cstdlib>

#define NDEBUG

#include <glog/logging.h>

#include "doc_length_store.h"
#include "intersect.h"
#include "scoring.h"
#include "utils.h"
#include "posting_list_vec.h"
#include "qq_mem_uncompressed_engine.h"


utils::ResultRow score_bench(const int &n_terms, const int &n_docs) {
  const int n_repeats = 100000;

  IntersectionResult result;
  std::vector<StandardPosting> postings(n_docs * n_terms);

  int i = 0;
  for (int doc_id = 0; doc_id < n_docs; doc_id++) {
    for (int term_id = 0; term_id < n_terms; term_id++) {
      postings[i] = StandardPosting(doc_id, 3); // term freq is 3
      result.SetPosting(doc_id, std::to_string(term_id), &postings[i]);

      i++;
    }
  }

  for (int term_id = 0; term_id < n_terms; term_id++) {
    result.SetDocCount(std::to_string(term_id), 10);
  }

  DocLengthStore lengths_store;
  for (int doc_id = 0; doc_id < n_docs; doc_id++) {
    lengths_store.AddLength(doc_id, 100);
  }

  auto start = utils::now();
  for (int i = 0; i < n_repeats; i++) {
    auto scores = score_docs(result, lengths_store);
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  assert(scores.size() == n_docs);

  
  utils::ResultRow row;
  row["n_terms"] = std::to_string(n_terms);
  row["n_docs"] = std::to_string(n_docs);
  row["duration"] = std::to_string(dur / n_repeats);

  return row;
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

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  score_bench_suite();
}

