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
#include "general_config.h"
#include "query_pool.h"

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


std::unique_ptr<QqMemUncompressedEngine> create_engine_from_file(
    const GeneralConfig &config) {
  std::unique_ptr<QqMemUncompressedEngine> engine(new QqMemUncompressedEngine);
  engine->LoadLocalDocuments(config.GetString("linedoc_path"), 
                             config.GetInt("n_docs"),
                             config.GetString("loader"));
  std::cout << "Term Count: " << engine->TermCount() << std::endl;
  return engine;
}


utils::ResultRow search(QqMemUncompressedEngine *engine, 
                        const GeneralConfig &config) {
  const int n_repeats = config.GetInt("n_repeats");
  utils::ResultRow row;
  
  // construct query pool
  QueryPool query_pool;
  load_query_pool(&query_pool, config);

  auto enable_snippets = config.GetBool("enable_snippets");
  std::cout << "Construct query pool successfully" << std::endl;
  auto start = utils::now();
  for (int i = 0; i < n_repeats; i++) {
    // auto terms = query_pool.Next();
    // for (auto term: terms) {
      // std::cout << term << " ";
    // }
    // auto query = SearchQuery(terms, enable_snippets);
    auto query = SearchQuery(query_pool.Next(), enable_snippets);
    
    query.n_snippet_passages = config.GetInt("n_passages");
    auto result = engine->Search(query);
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  row["duration"] = std::to_string(dur / n_repeats); 
  row["QPS"] = std::to_string(n_repeats / dur);
  return row;
}


void qq_uncompressed_bench() {
  utils::ResultTable table;
  auto engine = create_engine(1, 2, 425);

  // table.Append(search(engine.get(), TermList{"0"}));
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


void qq_uncompressed_bench_wiki(const GeneralConfig &config) {
  utils::ResultTable table;

  auto engine = create_engine_from_file(config);

  auto row = search(engine.get(), config);

  row["n_docs"] = std::to_string(config.GetInt("n_docs"));
  row["query_source"] = config.GetString("query_source");
  if (row["query_source"] == "hardcoded") {
    auto terms = config.GetStringVec("terms");
    for (auto term : terms) {
      row["query"] += term;
    }
  }
  table.Append(row);

  std::cout << table.ToStr();
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  GeneralConfig config;
  config.SetInt("n_docs", 10000);
  // config.SetString("linedoc_path", 
      // "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc");
  // config.SetString("loader", "with-offsets");

  config.SetString("linedoc_path", 
      "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc");
  config.SetString("loader", "naive");

  config.SetInt("n_repeats", 500000);
  config.SetInt("n_passages", 3);
  config.SetBool("enable_snippets", true);
  //config.SetBool("enable_snippets", false);
  
  
  config.SetString("query_source", "hardcoded");
  config.SetStringVec("terms", std::vector<std::string>{"hello"});
  //config.SetStringVec("terms", std::vector<std::string>{"barack", "obama"});
  //config.SetStringVec("terms", std::vector<std::string>{"len", "from", "mai"});
  // config.SetStringVec("terms", std::vector<std::string>{"arsen"});

  // config.SetString("query_source", "querylog");
  // config.SetString("querylog_path", "/mnt/ssd/downloads/test_querylog");

  qq_uncompressed_bench_wiki(config);

  // qq_uncompressed_bench();
}


