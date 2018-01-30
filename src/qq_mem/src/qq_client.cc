#include <chrono>
#include <thread>

#include "grpc_client_impl.h"
#include "general_config.h"


void bench_async_client(const int n_threads) {
  GeneralConfig config;
  config.SetString("synchronization", "ASYNC");
  config.SetString("rpc_arity", "STREAMING");
  config.SetString("target", "localhost:50051");
  config.SetInt("n_client_channels", 64);
  config.SetInt("n_rpcs_per_channel", 100);
  config.SetInt("n_messages_per_call", 100000);
  config.SetInt("n_threads", n_threads); 
  config.SetInt("n_threads_per_cq", 1);
  config.SetInt("benchmark_duration", 10);
  config.SetBool("save_reply", false);
  // config.SetBool("save_reply", true);

  auto query_pool_array = create_query_pool_array(TermList{"hello"}, 
      config.GetInt("n_threads"));
  // auto query_pool_array = create_query_pool_array(
      // "/mnt/ssd/downloads/wiki_QueryLog_tokenized",
      // config.GetInt("n_threads"));

  auto async_client = CreateClient(config, std::move(query_pool_array));
  async_client->Wait();
  async_client->ShowStats();
}

void bench_sync_client(const int n_threads, std::string arity) {
  GeneralConfig config;
  config.SetString("synchronization", "SYNC");
  config.SetString("rpc_arity", arity);
  config.SetString("target", "localhost:50051");
  config.SetInt("n_client_channels", 64);
  config.SetInt("n_threads", n_threads); 
  config.SetInt("benchmark_duration", 10);
  // config.SetBool("save_reply", true);
  config.SetBool("save_reply", false);

  auto query_pool_array = create_query_pool_array(TermList{"hello"}, 
      config.GetInt("n_threads"));
  // auto query_pool_array = create_query_pool_array(
      // "/mnt/ssd/downloads/wiki_QueryLog_tokenized",
      // config.GetInt("n_threads"));

  auto async_client = CreateClient(config, std::move(query_pool_array));
  async_client->Wait();
  async_client->ShowStats();
}

void sanity_check() {
  // Search synchroniously, as a sanity check
  auto client = CreateSyncClient("localhost:50051");
  std::vector<int> doc_ids;
  bool ret;
  ret = client->Search("multicellular", doc_ids);
  assert(ret == true);
  assert(doc_ids.size() >= 1);
}

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cout << "Usage: exefile threads sync_type arity" << std::endl;
    exit(1);
  }

  int n_threads = std::atoi(argv[1]);
  std::string sync_type = argv[2];
  auto arity = argv[3];

  sanity_check();
  utils::sleep(1);

  if (sync_type == "ASYNC") {
    bench_async_client(n_threads);
  } else if (sync_type == "SYNC") {
    bench_sync_client(n_threads, arity);
  } else {
    throw std::runtime_error("wrong sync type" + sync_type);
  }

  return 0;
}

