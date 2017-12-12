#include <iostream>
#include <string>
#include <vector>

#include "qq_engine.h"
#include "utils.h"

const int K = 1000;
const int M = 1000 * K;
const int B = 1000 * M;

int main(int argc, char **argv) {
  const int n_queries = 1 * M;
  const int n_rows = 1 * M;
  std::vector<int> doc_ids; 

  QQSearchEngine engine;
  engine.LoadLocalDocuments(
      "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc", n_rows);

  auto start = utils::now();
  for (int i = 0; i < n_queries; i++) {
    doc_ids = engine.Search(TermList{"hello"}, SearchOperator::AND);
    // std::cout << "doc_ids.size()=" << doc_ids.size() << std::endl;
  }
  auto end = utils::now();
  auto duration = utils::duration(start, end);

  std::cout 
    << "n_queries: " << n_queries << std::endl
    << "duration: " << duration << std::endl
    << "QPS: " << n_queries / duration << std::endl;
}


