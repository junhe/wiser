#include <chrono>
#include <thread>

#include "grpc_client_impl.h"
#include "index_creator.h"
#include "general_config.h"


int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "Usage: exefile threads" << std::endl;
    exit(1);
  }

  int n_threads = std::atoi(argv[1]);

  // Search synchroniously, as a sanity check
  auto client = CreateSyncClient("localhost:50051");
  std::vector<int> doc_ids;
  bool ret;
  ret = client->Search("multicellular", doc_ids);
  assert(ret == true);
  assert(doc_ids.size() >= 1);

  utils::sleep(1);

  GeneralConfig config;
  config.SetString("target", "localhost:50051");
  config.SetInt("n_client_channels", 64);
  config.SetInt("n_rpcs_per_channel", 100);
  config.SetInt("n_messages_per_call", 100000);
  config.SetInt("n_async_threads", n_threads); 
  config.SetInt("n_threads_per_cq", 1);
  config.SetInt("benchmark_duration", 5);

  auto query_pool_array = create_query_pool_array(TermList{"hello"}, 
      config.GetInt("n_async_threads"));
  // auto query_pool_array = create_query_pool_array(
      // "/mnt/ssd/downloads/wiki_QueryLog_tokenized",
      // config.GetInt("n_async_threads"));

  auto async_client = CreateAsyncClient(config, std::move(query_pool_array));
  async_client->Wait();
  async_client->ShowStats();
  return 0;
}

