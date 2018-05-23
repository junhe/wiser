#include <glog/logging.h>
#include <gflags/gflags.h>

#include "general_config.h"

#include "engine_factory.h"
#include "qq_mem_engine.h"
#include "utils.h"

// This program reads a line doc, convert them to qq memory dump format


std::unique_ptr<SearchEngineServiceNew> 
    CreateQqEngineFromFile(GeneralConfig config) 
{
  std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
      config.GetString("engine_type"));

  if (config.GetString("load_source") == "linedoc") {
    engine->LoadLocalDocuments(config.GetString("linedoc_path"), 
                               config.GetInt("n_docs"),
                               config.GetString("loader"));
  } else {
    std::cout << "You must speicify load_source" << std::endl;
  }

  std::cout << "Term Count: " << engine->TermCount() << std::endl;
  return engine;
}


GeneralConfig config_by_jun() {
  GeneralConfig config;
  config.SetString("engine_type", "qq_mem_compressed");

  config.SetString("load_source", "linedoc");
  // config.SetString("load_source", "dump");

  config.SetInt("n_docs", 100);
  config.SetString("linedoc_path", "/mnt/ssd/wiki/line_doc.toy");
  config.SetString("loader", "WITH_POSITIONS");

  config.SetInt("n_queries", 1000);
  config.SetInt("n_passages", 3);
  config.SetBool("enable_snippets", true);
  
  config.SetString("query_source", "hardcoded");
  config.SetStringVec("terms", std::vector<std::string>{"hello"});

  return config;
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto config = config_by_jun();

  auto engine = CreateQqEngineFromFile(config); 

  engine->Serialize("/mnt/ssd/tmp");
}


