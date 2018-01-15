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


std::unique_ptr<QqMemUncompressedEngine> create_engine_from_file(const int n_docs) {
  std::unique_ptr<QqMemUncompressedEngine> engine(new QqMemUncompressedEngine);
  //engine->LoadLocalDocuments("/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc", n_docs);
  engine->LoadLocalDocuments("/mnt/ssd/downloads/wiki_tokenized.linedoc", n_docs);
  //engine->LoadLocalDocuments("/mnt/ssd/downloads/test.linedoc", n_docs);
  std::cout << "Term Count: " << engine->TermCount() << std::endl;
  return engine;
}

utils::ResultRow search(QqMemUncompressedEngine *engine, const TermList &terms) {
  const int n_repeats = 10000;
  //const int n_repeats = 1;
  utils::ResultRow row;

  auto start = utils::now();
  for (int i = 0; i < n_repeats; i++) {
    // auto doc_ids = engine->SearchWithoutSnippet(terms);
    auto result = engine->Search(SearchQuery(terms, true));
    //std::cout << result.ToStr() << std::endl;
    // auto result = engine->Search(SearchQuery(terms, false));
    // auto result = engine->ProcessQueryTogether(SearchQuery(terms, false));
    // auto result = engine->ProcessQueryTogether(SearchQuery(terms, true));
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  row["duration"] = std::to_string(dur / n_repeats); 
  row["QPS"] = std::to_string(n_repeats / dur);

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
  auto engine = create_engine(1, 2, 425);

  table.Append(search(engine.get(), TermList{"0"}));
  // table.Append(search(engine.get(), TermList{"100"}));
  // table.Append(search(engine.get(), TermList{"500"}));
  // table.Append(search(engine.get(), TermList{"980"}));
  // table.Append(search(engine.get(), TermList{"990"}));
  // table.Append(search(engine.get(), TermList{"995"})); // 500 docs
  // table.Append(search(engine.get(), TermList{"999"}));

  // table.Append(search(engine.get(), TermList{"0", "1"}));
  // table.Append(search(engine.get(), TermList{"500", "501"}));
  // table.Append(search(engine.get(), TermList{"994", "995"}));
  // table.Append(search(engine.get(), TermList{"998", "999"}));

  std::cout << table.ToStr();
}


void qq_uncompressed_bench_wiki() {
  utils::ResultTable table;

  const int n_docs = 10000000;
  //const int n_docs = 10000;
  auto engine = create_engine_from_file(n_docs);

  auto row = search(engine.get(), TermList{"hello"});
  //auto row = search(engine.get(), TermList{"barack", "obama"});
  //auto row = search(engine.get(), TermList{"len", "from", "mai"});
  row["n_docs"] = std::to_string(n_docs);
  table.Append(row);

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


