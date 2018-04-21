#include <iostream>
#include <cassert>
#include <unordered_map>
#include <sstream>
#include <cstdlib>

#include <glog/logging.h>

#include "query_processing.h"
#include "scoring.h"
#include "utils.h"
#include "qq_mem_engine.h"


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

void sorting_bench_suite() {
  std::srand(1);

  utils::ResultTable table;
  table.Append(sorting_bench(1000, 10));
  table.Append(sorting_bench(10000, 10));
  table.Append(sorting_bench(100000, 10));
  table.Append(sorting_bench(1000000, 10));
  table.Append(sorting_bench(1000000, 100));
  table.Append(sorting_bench(1000000, 1000));

  std::cout << table.ToStr();
}


int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  sorting_bench_suite();
}

