#include "grpc_server_impl.h"


std::unique_ptr<AsyncServer> CreateServer(const std::string &target, 
    int n_threads_per_cq, int n_server_threads, int n_secs) {

  GeneralConfig config;
  config.SetString("target", target);
  config.SetInt("n_threads_per_cq", n_threads_per_cq);
  config.SetInt("n_server_threads", n_server_threads);
  config.SetInt("server_duration", n_secs);
  config.SetString("line_doc_path", "");
  config.SetInt("n_line_doc_rows", 0);

  std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
      "qq_mem_uncompressed");

  std::unique_ptr<AsyncServer> server(new AsyncServer(config, std::move(engine)));
  return server;
}


std::unique_ptr<AsyncServer> CreateServer(const std::string &target, 
    int n_threads_per_cq, int n_server_threads, int n_secs,
    const std::string &line_doc_path, const int &n_rows) {

  GeneralConfig config;
  config.SetString("target", target);
  config.SetInt("n_threads_per_cq", n_threads_per_cq);
  config.SetInt("n_server_threads", n_server_threads);
  config.SetInt("server_duration", n_secs);
  config.SetString("line_doc_path", line_doc_path);
  config.SetInt("n_line_doc_rows", n_rows);

  std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
      "qq_mem_uncompressed");

  std::unique_ptr<AsyncServer> server(new AsyncServer(config, std::move(engine)));
  return server;
}




