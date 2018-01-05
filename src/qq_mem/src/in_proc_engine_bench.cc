#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>

#define NDEBUG
#include <glog/logging.h>

#include "qq_engine.h"
#include "qq_mem_uncompressed_engine.h"
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

void old_engine_bench() {
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


std::unique_ptr<QqMemUncompressedEngine> create_engine(const int &step_height, 
    const int &step_width, const int &n_steps) {
  std::unique_ptr<QqMemUncompressedEngine> engine(new QqMemUncompressedEngine);

  utils::Staircase staircase(step_height, step_width, n_steps);

  std::string doc;
  int cnt = 0;
  while ( (doc = staircase.NextLayer()) != "" ) {
    // std::cout << doc << std::endl;
    engine->AddDocument(doc, doc);
    cnt++;

    if (cnt % 1000 == 0) {
      std::cout << cnt << std::endl;
    }
  }

  return engine;
}


std::unique_ptr<QqMemUncompressedEngine> create_engine_from_file() {
  std::unique_ptr<QqMemUncompressedEngine> engine(new QqMemUncompressedEngine);
  engine->LoadLocalDocuments("/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc", 10000000);
  std::cout << "Term Count: " << engine->TermCount() << std::endl;
  return engine;
}

utils::ResultRow search(QqMemUncompressedEngine *engine, const TermList &terms) {
  const int n_repeats = 10;
  utils::ResultRow row;

  auto start = utils::now();
  for (int i = 0; i < n_repeats; i++) {
    auto doc_ids = engine->SearchWithoutSnippet(terms);
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  row["duration"] = std::to_string(dur / n_repeats); 

  std::string query;
  for (int i = 0; i < terms.size(); i++) {
    query += terms[i];
    if (i != terms.size() - 1) {
      query += "+";
    }
  }
  row["query"] = query;

  return row;
}


void qq_uncompressed_bench() {
  utils::ResultTable table;
  auto engine = create_engine(100, 1, 1000);

  table.Append(search(engine.get(), TermList{"0"}));
  table.Append(search(engine.get(), TermList{"100"}));
  table.Append(search(engine.get(), TermList{"500"}));
  table.Append(search(engine.get(), TermList{"980"}));
  table.Append(search(engine.get(), TermList{"990"}));
  table.Append(search(engine.get(), TermList{"995"})); // 500 docs
  table.Append(search(engine.get(), TermList{"999"}));

  table.Append(search(engine.get(), TermList{"0", "1"}));
  table.Append(search(engine.get(), TermList{"500", "501"}));
  table.Append(search(engine.get(), TermList{"994", "995"}));
  table.Append(search(engine.get(), TermList{"998", "999"}));

  std::cout << table.ToStr();
}


void qq_uncompressed_bench_wiki() {
  utils::ResultTable table;
  auto engine = create_engine_from_file();

  table.Append(search(engine.get(), TermList{"hello"}));
  std::cout << table.ToStr();
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  // qq_uncompressed_bench();
  qq_uncompressed_bench_wiki();
}

