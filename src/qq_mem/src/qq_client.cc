#include <chrono>
#include <thread>

#include "grpc_client_impl.h"
#include "index_creator.h"


void make_queries(int n_queries) {
    std::string reply;

    auto qqengine = CreateSyncClient("localhost:50051");


    for (int i = 0; i < n_queries; i++) {
        std::vector<int> doc_ids;
        qqengine->Search("hello", doc_ids);
    }

}




int main(int argc, char** argv) {
  // Create index on server side
  // IndexCreator index_creator(
        // "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc", *client);
        // "src/testdata/enwiki-abstract_tokenized.linedoc.sample", *client);
  // index_creator.DoIndex(100000);

  // Search synchroniously
  auto client = CreateSyncClient("localhost:50051");
  std::vector<int> doc_ids;
  bool ret;
  ret = client->Search("multicellular", doc_ids);
  assert(ret == true);
  assert(doc_ids.size() >= 1);

  utils::sleep(1);

  auto async_client = CreateAsyncClient("localhost:50051", 
      64,  // n channels
      100,  // rpcs per channel
      100000,  // messages per rpc
      16,  // n threads
      1,  // thread per cq
      30); // duration (seconds)
  async_client->Wait();
  async_client->ShowStats();
  return 0;
}

