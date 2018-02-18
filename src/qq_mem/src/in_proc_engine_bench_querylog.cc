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
#include "grpc_client_impl.h"
#include "general_config.h"
#include "query_pool.h"

const int K = 1000;
const int M = 1000 * K;
const int B = 1000 * M;

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


struct Treatment {
  Treatment(TermList terms_in, bool is_phrase_in, int n_repeats_in)
    :terms(terms_in), is_phrase(is_phrase_in), n_repeats(n_repeats_in) {}
  TermList terms;
  bool is_phrase;
  int n_repeats;
};


class TreatmentExecutor {
 public:
  virtual utils::ResultRow Execute(Treatment treatment) = 0;
};

class GrpcTreatmentExecutor: public TreatmentExecutor {
 public:
  GrpcTreatmentExecutor(int n_threads){
    // ASYNC Streaming
    // client_config_.SetString("synchronization", "SYNC");
    // client_config_.SetString("rpc_arity", "STREAMING");
    // client_config_.SetString("target", "localhost:50051");
    // client_config_.SetInt("n_client_channels", 64);
    // client_config_.SetInt("n_rpcs_per_channel", 100);
    // client_config_.SetInt("n_messages_per_call", 100000);
    // client_config_.SetInt("n_threads", n_threads); 
    // client_config_.SetInt("n_threads_per_cq", 1);
    // client_config_.SetInt("benchmark_duration", 5);
    // client_config_.SetBool("save_reply", false);

    client_config_.SetString("synchronization", "SYNC");
    client_config_.SetString("rpc_arity", "STREAMING");
    client_config_.SetString("target", "localhost:50051");
    client_config_.SetInt("n_client_channels", 64);
    client_config_.SetInt("n_threads", n_threads); 
    client_config_.SetInt("benchmark_duration", 10);
    // client_config_.SetBool("save_reply", true);
    client_config_.SetBool("save_reply", false);

  }

  std::unique_ptr<QueryProducer> CreateProducer(Treatment treatment) {
    GeneralConfig config;
    config.SetBool("is_phrase", treatment.is_phrase);
    return CreateQueryProducer(treatment.terms, 
        client_config_.GetInt("n_threads"), config);
  }

  utils::ResultRow Execute(Treatment treatment) {
    utils::ResultRow row;

    auto client = CreateClient(client_config_, CreateProducer(treatment));
    client->Wait();
    row = client->ShowStats();

    return row;
  }

 private:
  GeneralConfig client_config_;
};

class LocalTreatmentExecutor: public TreatmentExecutor {
 public:
  LocalTreatmentExecutor(SearchEngineServiceNew *engine)
     :engine_(engine) {}

  std::unique_ptr<QueryProducer> CreateProducer(Treatment treatment) {
    GeneralConfig config;
    config.SetBool("is_phrase", treatment.is_phrase);
    return CreateQueryProducer(treatment.terms, 1, config);
  }

  utils::ResultRow Execute(Treatment treatment) {
    const int n_repeats = treatment.n_repeats;
    utils::ResultRow row;
    auto query_producer = CreateProducer(treatment);

    auto start = utils::now();
    for (int i = 0; i < n_repeats; i++) {
      auto query = query_producer->NextNativeQuery(0);
      // query.n_snippet_passages = config_.GetInt("n_passages");
      auto result = engine_->Search(query);

      // std::cout << result.ToStr() << std::endl;
    }
    auto end = utils::now();
    auto dur = utils::duration(start, end);

    row["latency"] = std::to_string(dur / n_repeats); 
    row["duration"] = std::to_string(dur); 
    row["QPS"] = std::to_string(round(100 * n_repeats / dur) / 100.0);
    return row;
  }

 private:
  SearchEngineServiceNew *engine_;
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
    std::vector<Treatment> treatments {
      Treatment({"hello"}, false, 1000),
      Treatment({"from"}, false, 20),
      Treatment({"ripdo"}, false, 10000),

      Treatment({"hello", "world"}, false, 100),
      Treatment({"from", "also"}, false, 10),
      Treatment({"ripdo", "liftech"}, false, 1000000),

      Treatment({"hello", "world"}, true, 100),
      Treatment({"barack", "obama"}, true, 100),
      Treatment({"from", "also"}, true, 10) 
    };

    treatments_ = treatments;
    for (auto &query : treatments) {
      GeneralConfig config;
      config.SetBool("is_phrase", query.is_phrase);
      query_producers_.push_back(
          CreateQueryProducer(query.terms, 1, config));
    }
  }

  void Before() {
    // engine_ = std::move(CreateEngineFromFile());
    // treatment_executor_.reset(new LocalTreatmentExecutor(engine_.get()));

    treatment_executor_.reset(new GrpcTreatmentExecutor(16));//TODO
  }

  void RunTreatment(const int run_id) {
    std::cout << "Running treatment " << run_id << std::endl;

    // auto row = Search(query_producers_[run_id].get(), run_id);
    auto row = treatment_executor_->Execute(treatments_[run_id]);

    row["n_docs"] = std::to_string(config_.GetInt("n_docs"));
    row["terms"] = Concat(treatments_[run_id].terms);
    row["is_phrase"] = std::to_string(treatments_[run_id].is_phrase);
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
      auto start = utils::now();
      engine->Deserialize(config_.GetString("dump_path"));
      auto end = utils::now();
      std::cout << "Time to load dumpped data: " << utils::duration(start, end) << std::endl;
    } else {
      std::cout << "You must speicify load_source" << std::endl;
    }

    // engine->Serialize("/mnt/ssd/big-engine-dump");

    std::cout << "Term Count: " << engine->TermCount() << std::endl;
    return engine;
  }

  void ShowEngineStatus(SearchEngineServiceNew *engine, const TermList &terms) {
    auto pl_sizes = engine->PostinglistSizes(terms); 
    for (auto pair : pl_sizes) {
      std::cout << pair.first << " : " << pair.second << std::endl;
    }
  }

 private:
  GeneralConfig config_;
  std::vector<std::unique_ptr<QueryProducer>> query_producers_;
  std::unique_ptr<SearchEngineServiceNew> engine_;
  std::vector<Treatment> treatments_;
  utils::ResultTable table_;
  std::unique_ptr<TreatmentExecutor> treatment_executor_;
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

  // config.SetString("load_source", "linedoc");
  config.SetString("load_source", "dump");

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


