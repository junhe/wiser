#include <chrono>
#include <thread>

#include "qq_client.h"
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
  auto client = CreateAsyncClient("localhost:50051", 64, 100, 1000, 8, 1, 8);
  client->Wait();
  return 0;
}

