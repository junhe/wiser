#include <glog/logging.h>
#include <gflags/gflags.h>

#include "general_config.h"

#include "engine_factory.h"
#include "qq_mem_engine.h"
#include "utils.h"

// This program reads a line doc, convert them to qq memory dump format

DEFINE_string(line_doc_path, "", "path of the line doc");
DEFINE_string(dump_dir_path, "", "path of the directory to dump to");

std::unique_ptr<SearchEngineServiceNew> 
    CreateQqEngineFromFile(GeneralConfig config) 
{
  std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
      config.GetString("engine_type"));

  if (config.GetString("load_source") == "linedoc") {
    engine->LoadLocalDocuments(config.GetString("line_doc_path"), 
                               config.GetInt("n_docs"),
                               config.GetString("loader"));
  } else {
    std::cout << "You must speicify load_source" << std::endl;
  }

  std::cout << "Term Count: " << engine->TermCount() << std::endl;
  return engine;
}


void CheckArgs() {
  if (FLAGS_line_doc_path == "") {
    LOG(FATAL) << "Arg line_doc_path is not set";
  }

  if (FLAGS_dump_dir_path == "") {
    LOG(FATAL) << "Arg dump_dir_path is not set";
  }
}


GeneralConfig config_by_jun() {
  GeneralConfig config;
  config.SetString("engine_type", "qq_mem_compressed");

  config.SetString("load_source", "linedoc");

  config.SetInt("n_docs", 100000000);
  config.SetString("line_doc_path", FLAGS_line_doc_path);
  config.SetString("dump_dir_path", FLAGS_dump_dir_path);
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

  CheckArgs();

  auto config = config_by_jun();

  auto engine = CreateQqEngineFromFile(config); 

  engine->Serialize(config.GetString("dump_dir_path"));
}


