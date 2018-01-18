#include "catch.hpp"

#include "qq.pb.h"

#include "unifiedhighlighter.h"
#include "intersect.h"
#include "utils.h"
#include "general_config.h"
#include "query_pool.h"
#include "histogram.h"
#include "grpc_server_impl.h"
#include "grpc_client_impl.h"
#include <grpc/support/histogram.h>


TEST_CASE( "GRPC Sync Client and Server", "[grpc]" ) {
  auto server = CreateServer(std::string("localhost:50051"), 1, 1, 0);
  utils::sleep(1); // warm up the server

  auto client = CreateSyncClient("localhost:50051");

  SECTION("Echo") {
    EchoData request;
    request.set_message("hello");
    EchoData reply;
    auto ret = client->Echo(request, reply);

    REQUIRE(ret == true);
    REQUIRE(reply.message() == "hello");
    // server will automatically destructed here.
  }

  SECTION("Add documents and search") {
    std::vector<int> doc_ids;
    bool ret;

    ret = client->Search("body", doc_ids);
    REQUIRE(doc_ids.size() == 0);
    REQUIRE(ret == true);

    client->AddDocument("my title", "my url", "my body");
    client->AddDocument("my title", "my url", "my spirit");

    SECTION("Use the sync client to search") {
      std::vector<int> doc_ids;
      client->Search("body", doc_ids);
      REQUIRE(ret == true);
      REQUIRE(doc_ids.size() == 1);
    }
 
    SECTION("Use the async client to search") {
      // The same check as the section above just to make sure
      // we have the documents indexed.
      std::vector<int> doc_ids;
      client->Search("body", doc_ids);
      REQUIRE(ret == true);
      REQUIRE(doc_ids.size() == 1);

      GeneralConfig config;
      config.SetString("target", "localhost:50051");
      config.SetInt("n_client_channels", 1);
      config.SetInt("n_rpcs_per_channel", 1);
      config.SetInt("n_messages_per_call", 1);
      config.SetInt("n_async_threads", 8); 
      config.SetInt("n_threads_per_cq", 1);
      config.SetInt("benchmark_duration", 2);
      config.SetBool("save_reply", true);

      auto query_pool_array = create_query_pool_array(TermList{"body"}, 
          config.GetInt("n_async_threads"));
      auto client = CreateAsyncClient(config, std::move(query_pool_array));
      client->Wait();
      auto reply_pools = client->GetReplyPools();

      for (int i = 0; i < config.GetInt("n_async_threads"); i++) {
        std::cout << "reply pool size: " << reply_pools->at(i).size() << std::endl;
      }


      REQUIRE(reply_pools->size() == config.GetInt("n_async_threads"));
      REQUIRE(reply_pools->at(0).size() > 0);


      REQUIRE(reply_pools->at(0).at(0).entries_size() == 1);
      REQUIRE(reply_pools->at(1).size() > 0);
      REQUIRE(reply_pools->at(1).at(0).entries_size() == 1);


      client.release();
    }
    // server will automatically destructed here.
  }
}


TEST_CASE( "GRPC Async Client and Server", "[grpc]" ) {
  auto server = CreateServer(std::string("localhost:50051"), 1, 4, 0);
  utils::sleep(1); // warm up the server

  // Use another thread to create client
  // std::thread client_thread(run_client);

  GeneralConfig config;
  config.SetString("target", "localhost:50051");
  config.SetInt("n_client_channels", 64);
  config.SetInt("n_rpcs_per_channel", 100);
  config.SetInt("n_messages_per_call", 100);
  config.SetInt("n_async_threads", 8); 
  config.SetInt("n_threads_per_cq", 1);
  config.SetInt("benchmark_duration", 2);
  config.SetBool("save_reply", true);

  auto query_pool_array = create_query_pool_array(TermList{"hello"}, 
      config.GetInt("n_async_threads"));
  auto client = CreateAsyncClient(config, std::move(query_pool_array));
  client->Wait();
  client.release();
}





