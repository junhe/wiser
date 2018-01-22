#include "grpc_server_impl.h"


std::unique_ptr<AsyncServer> CreateServer(const std::string &target, 
    int n_threads_per_cq, int n_server_threads, int n_secs) {

  ConfigType config;
  config["target"] = target;
  config["n_threads_per_cq"] = std::to_string(n_threads_per_cq);
  config["n_server_threads"] = std::to_string(n_server_threads);
  config["server_duration"] = std::to_string(n_secs);
  config["line_doc_path"] = "";
  config["n_line_doc_rows"] = "";

  std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
      "qq_mem_uncompressed");

  std::unique_ptr<AsyncServer> server(new AsyncServer(config, std::move(engine)));
  return server;
}


std::unique_ptr<AsyncServer> CreateServer(const std::string &target, 
    int n_threads_per_cq, int n_server_threads, int n_secs,
    const std::string &line_doc_path, const int &n_rows) {

  ConfigType config;
  config["target"] = target;
  config["n_threads_per_cq"] = std::to_string(n_threads_per_cq);
  config["n_server_threads"] = std::to_string(n_server_threads);
  config["server_duration"] = std::to_string(n_secs);
  config["line_doc_path"] = line_doc_path;
  config["n_line_doc_rows"] = std::to_string(n_rows);

  std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
      "qq_mem_uncompressed");

  std::unique_ptr<AsyncServer> server(new AsyncServer(config, std::move(engine)));
  return server;
}




