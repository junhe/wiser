#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>

#define NDEBUG
#include <glog/logging.h>

#include "qq_mem_engine.h"
#include "utils.h"
#include "general_config.h"
#include "query_pool.h"

const int K = 1000;
const int M = 1000 * K;
const int B = 1000 * M;


class Experiment {
 public:
  virtual void Before() {}
  virtual void BeforeEach(const int run_id) {}
  virtual void RunTreatment(const int run_id) = 0;
  virtual void AfterEach(const int run_id) {}
  virtual void After() {}

  virtual int NumberOfRuns() = 0;
  virtual void Run() {
    Before();
    for (int i = 0; i < NumberOfRuns(); i++) {
      BeforeEach(i);
      RunTreatment(i);
      AfterEach(i);
    }
    After();
  }
};


struct SimpleQuery {
  SimpleQuery(TermList terms_in, bool is_phrase_in)
    :terms(terms_in), is_phrase(is_phrase_in) {}
  TermList terms;
  bool is_phrase;
};

class InProcExperiment: public Experiment {
 public:
  InProcExperiment(GeneralConfig config): config_(config) {
    MakeQueryProducers();
  }

  int NumberOfRuns() {
    return query_producers_.size();
  }

  void MakeQueryProducers() {
    // single term
    std::vector<SimpleQuery> queries {
      SimpleQuery({"hello"}, false),
      SimpleQuery({"from"}, false),
      SimpleQuery({"ripdo"}, false),

      SimpleQuery({"hello", "world"}, false),
      SimpleQuery({"from", "also"}, false),
      SimpleQuery({"ripdo", "liftech"}, false),

      SimpleQuery({"hello", "world"}, true),
      SimpleQuery({"barack", "obama"}, true),
      SimpleQuery({"from", "also"}, true) 
    };

    simple_queries_ = queries;
    for (auto &query : queries) {
      GeneralConfig config;
      config.SetBool("is_phrase", query.is_phrase);
      query_producers_.push_back(
          CreateQueryProducer(query.terms, 1, config));
    }
  }

  void Before() {
    engine_ = std::move(CreateEngineFromFile());
  }

  std::string Concat(TermList terms) {
    std::string s;
    for (int i = 0; i < terms.size(); i++) {
      s += terms[i];
      if (i < terms.size() - 1) {
        // not the last term
        s += "+";
      }
    }
    return s;
  }

  void RunTreatment(const int run_id) {
    std::cout << "Running treatment " << run_id << std::endl;

    auto row = Search(query_producers_[run_id].get());

    row["n_docs"] = std::to_string(config_.GetInt("n_docs"));
    row["terms"] = Concat(simple_queries_[run_id].terms);
    row["is_phrase"] = std::to_string(simple_queries_[run_id].is_phrase);
    table_.Append(row);
  }

  void After() {
    std::cout << table_.ToStr();
  }

  std::unique_ptr<SearchEngineServiceNew> CreateEngineFromFile() {
    std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
        config_.GetString("engine_type"));

    if (config_.GetString("load_source") == "linedoc") {
      engine->LoadLocalDocuments(config_.GetString("linedoc_path"), 
                                 config_.GetInt("n_docs"),
                                 config_.GetString("loader"));
    } else if (config_.GetString("load_source") == "dump") {
      std::cout << "Loading engine from dumpped data...." << std::endl;
      engine->Deserialize(config_.GetString("dump_path"));
    } else {
      std::cout << "You must speicify load_source" << std::endl;
    }

    // engine->Serialize("/mnt/ssd/big-engine-dump");

    std::cout << "Term Count: " << engine->TermCount() << std::endl;
    return engine;
  }

  std::unique_ptr<SearchEngineServiceNew> CreateEngineBySyntheticData(
      const int &step_height, const int &step_width, const int &n_steps) {
    std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine("qq_mem_uncompressed");

    utils::Staircase staircase(step_height, step_width, n_steps);

    std::string doc;
    int cnt = 0;
    while ( (doc = staircase.NextLayer()) != "" ) {
      // std::cout << doc << std::endl;
      engine->AddDocument(DocInfo(doc, doc, "", "", "TOKEN_ONLY"));
      cnt++;

      if (cnt % 1000 == 0) {
        std::cout << cnt << std::endl;
      }
    }

    return engine;
  }

  void ShowEngineStatus(SearchEngineServiceNew *engine, const TermList &terms) {
    auto pl_sizes = engine->PostinglistSizes(terms); 
    for (auto pair : pl_sizes) {
      std::cout << pair.first << " : " << pair.second << std::endl;
    }
  }

  std::unique_ptr<QueryProducer> CreateQueryProducer2() {
    // Create query producer
    std::unique_ptr<TermPoolArray> array(new TermPoolArray(1));

    if (config_.GetString("query_source") == "hardcoded") {
      array->LoadTerms(config_.GetStringVec("terms"));
    } else {
      array->LoadFromFile(config_.GetString("querylog_path"), 100);
    }
    std::cout << "Constructed query pool successfully" << std::endl;

    GeneralConfig query_config;
    query_config.SetBool("return_snippets", true);
    query_config.SetBool("is_phrase", false);
    query_config.SetBool("return_snippets", config_.GetBool("enable_snippets"));

    std::unique_ptr<QueryProducer> query_producer(
        new QueryProducer(std::move(array), query_config));
    return query_producer;
  }

  utils::ResultRow Search(QueryProducer *query_producer) {
    const int n_repeats = config_.GetInt("n_repeats");
    utils::ResultRow row;

    auto start = utils::now();
    for (int i = 0; i < n_repeats; i++) {
      auto query = query_producer->NextNativeQuery(0);
      // query.n_snippet_passages = config_.GetInt("n_passages");
      auto result = engine_->Search(query);

      // std::cout << result.ToStr() << std::endl;
    }
    auto end = utils::now();
    auto dur = utils::duration(start, end);

    row["duration"] = std::to_string(dur / n_repeats); 
    row["QPS"] = std::to_string(n_repeats / dur);
    return row;
  }

 private:
  GeneralConfig config_;
  std::vector<std::unique_ptr<QueryProducer>> query_producers_;
  std::unique_ptr<SearchEngineServiceNew> engine_;
  std::vector<SimpleQuery> simple_queries_;
  utils::ResultTable table_;
};


GeneralConfig config_by_jun() {
/*
                                       /;    ;\
                                   __  \\____//
                                  /{_\_/   `'\____
                                  \___   (o)  (o  }
       _____________________________/          :--'   DRINKA
   ,-,'`@@@@@@@@       @@@@@@         \_    `__\
  ;:(  @@@@@@@@@        @@@             \___(o'o)
  :: )  @@@@          @@@@@@        ,'@@(  `===='        PINTA
  :: : @@@@@:          @@@@         `@@@:
  :: \  @@@@@:       @@@@@@@)    (  '@@@'
  ;; /\      /`,    @@@@@@@@@\   :@@@@@)                   MILKA
  ::/  )    {_----------------:  :~`,~~;
 ;;'`; :   )                  :  / `; ;
;;;; : :   ;                  :  ;  ; :                        DAY !!!
`'`' / :  :                   :  :  : :
    )_ \__;      ";"          :_ ;  \_\       `,','
    :__\  \    * `,'*         \  \  :  \   *  8`;'*  *
        `^'     \ :/           `^'  `-^-'   \v/ :  \/   -Bill Ames-

*/

  GeneralConfig config;
  config.SetString("engine_type", "qq_mem_compressed");
  // config.SetString("engine_type", "qq_mem_uncompressed");


  config.SetString("load_source", "linedoc");
  // config.SetString("load_source", "dump");

  config.SetInt("n_docs", 100);
  config.SetString("linedoc_path", 
      "/mnt/ssd/downloads/enwiki.linedoc_tokenized.1");
  config.SetString("loader", "WITH_POSITIONS");

  // config.SetString("linedoc_path", 
      // "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc");
  // config.SetString("loader", "TOKEN_ONLY");
  
  config.SetString("dump_path", "/mnt/ssd/big-engine-dump");

  config.SetInt("n_repeats", 1000);
  config.SetInt("n_passages", 3);
  // config.SetBool("enable_snippets", true);
  config.SetBool("enable_snippets", false);
  
  
  config.SetString("query_source", "hardcoded");
  config.SetStringVec("terms", std::vector<std::string>{"hello"});

  // config.SetString("query_source", "querylog");
  // config.SetString("querylog_path", "/mnt/ssd/downloads/wiki_QueryLog_tokenized");
  return config;
}

GeneralConfig config_by_kan() {
/*
           ____
         / |   |\
  _____/ @ |   | \
 |> . .    |   |   \
  \  .     |||||     \________________________
   |||||||\                                    )
            \                                 |
             \                                |
               \                             /
                |   ____________------\     |
                |  | |                ||    /
                |  | |                ||  |
                |  | |                ||  |
                |  | |                ||  |  Glo Pearl
               (__/_/                ((__/

*/

}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 4; 

  // Separate our configurations to avoid messes
  auto config = config_by_jun();
  // auto config = config_by_kan();
  
  // qq_uncompressed_bench_wiki(config);

  InProcExperiment experiment(config);
  experiment.Run();
}


