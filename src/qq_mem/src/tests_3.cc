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


TEST_CASE( "GRPC Client and Server", "[grpc]" ) {
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
      config.SetInt("n_client_channels", 100);
      config.SetInt("n_rpcs_per_channel", 10);
      config.SetInt("n_messages_per_call", 1);
      config.SetInt("n_async_threads", 2); 
      config.SetInt("n_threads_per_cq", 1);
      config.SetInt("benchmark_duration", 2);
      config.SetBool("save_reply", true);

      auto query_pool_array = create_query_pool_array(TermList{"body"}, 
          config.GetInt("n_async_threads"));
      auto client = CreateAsyncClient(config, std::move(query_pool_array));
      client->Wait();
      auto reply_pools = client->GetReplyPools();

      for (int i = 0; i < config.GetInt("n_async_threads"); i++) {
        LOG(INFO) << "reply pool size: " << reply_pools->at(i).size();
      }

      REQUIRE(reply_pools->size() == config.GetInt("n_async_threads"));
      auto n_replies_in_this_pool = reply_pools->at(0).size();
      auto this_pool = reply_pools->at(0);
      REQUIRE(n_replies_in_this_pool > 0);
      for (int i = 0; i < n_replies_in_this_pool; i++) {
        REQUIRE(this_pool.at(i).entries_size() == 1);
        REQUIRE(this_pool.at(i).entries(0).doc_id() == 0);
      }

      n_replies_in_this_pool = reply_pools->at(1).size();
      this_pool = reply_pools->at(1);
      REQUIRE(n_replies_in_this_pool > 0);
      for (int i = 0; i < n_replies_in_this_pool; i++) {
        REQUIRE(this_pool.at(i).entries_size() == 1);
        REQUIRE(this_pool.at(i).entries(0).doc_id() == 0);
      }

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


TEST_CASE( "Skip list works", "[postinglist]" ) {
  SECTION("HasSkip()") {
    PostingList_Vec<PostingSimple> pl("hello", 4);   
    for (int i = 0; i < 10; i++) {
      pl.AddPosting(PostingSimple(i, 88, Positions{28}));
    }

    SECTION("HasSkip()") {
      auto span = pl.GetSkipSpan();
      REQUIRE(span == 4);
    
      std::vector<int> has_skip{0, 4};
      for (auto i : has_skip) {
        REQUIRE(pl.HasSkip(i) == true);
      }

      std::vector<int> not_has_skip{1, 2, 3, 5, 6, 7, 8, 9};
      for (auto i : not_has_skip) {
        REQUIRE(pl.HasSkip(i) == false);
      }
    }

    SECTION("Skip list is correctly constructed") {
      auto *skip_list = pl.GetSkipList();
      REQUIRE(skip_list->size() == 2);
      REQUIRE(skip_list->at(0) == 4);
      REQUIRE(skip_list->at(1) == 8);
    }
  }
}




