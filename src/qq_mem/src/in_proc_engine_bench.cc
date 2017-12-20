#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <climits>

#include "qq_engine.h"
#include "utils.h"

const int K = 1000;
const int M = 1000 * K;
const int B = 1000 * M;


void make_queries(QQSearchEngine *engine, int n_millions) {
  std::vector<int> doc_ids; 
  for (int i = 0; i < n_millions; i++) {
    for (int j = 0; j < M; j++) {
      doc_ids = engine->Search(TermList{"hello"}, SearchOperator::AND);
      // volatile auto it = engine->Find(Term("hello"));
      // std::cout << "doc_ids.size()=" << doc_ids.size() << std::endl;
    }
  }
}


int main(int argc, char **argv) {
  const int n_millions_queries_per_thread = 1;
  const int n_threads = 32;
  const int n_rows = 1 * M;
  std::vector<std::thread> threadList;

  QQSearchEngine engine;
  engine.LoadLocalDocuments(
      "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc", n_rows);

  auto start = utils::now();
  for(int i = 0; i < n_threads; i++)
  {
    threadList.push_back( 
        std::thread( make_queries, &engine, n_millions_queries_per_thread) );
  }
  std::for_each(threadList.begin(), threadList.end(), std::mem_fn(&std::thread::join));

  auto end = utils::now();
  auto duration = utils::duration(start, end);

  int total_queries = n_millions_queries_per_thread * n_threads;
  std::cout 
    << "n_queries: " << total_queries << std::endl
    << "duration: " << duration << std::endl
    << "QPS (millions/sec): " << total_queries / duration << std::endl;
}


