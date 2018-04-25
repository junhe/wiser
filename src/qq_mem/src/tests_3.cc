#include "catch.hpp"

#include "qq.pb.h"

#include "engine_factory.h"
#include "highlighter.h"
#include "query_processing.h"
#include "utils.h"
#include "posting_list_vec.h"
#include "general_config.h"
#include "query_pool.h"
#include "histogram.h"
#include "grpc_server_impl.h"
#include "grpc_client_impl.h"
#include <grpc/support/histogram.h>

static std::unique_ptr<AsyncServer> CreateTestServer(const std::string &target, 
    int n_threads_per_cq, int n_server_threads, int n_secs) {

  GeneralConfig config;
  config.SetString("target", target);
  config.SetString("engine_name", "qq_mem_compressed");
  config.SetInt("n_threads_per_cq", n_threads_per_cq);
  config.SetInt("n_server_threads", n_server_threads);
  config.SetInt("server_duration", n_secs);
  config.SetString("line_doc_path", "");
  config.SetInt("n_line_doc_rows", 0);

  std::unique_ptr<SearchEngineServiceNew> engine = CreateSearchEngine(
      "qq_mem_compressed");


  std::unique_ptr<AsyncServer> server(new AsyncServer(config, std::move(engine)));
  return server;
}

TEST_CASE( "GRPC Client and Server", "[grpc]" ) {
	auto server = CreateTestServer(std::string("localhost:50051"), 1, 1, 0);
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
      config.SetInt("n_threads", 2); 
      config.SetInt("n_threads_per_cq", 1);
      config.SetInt("benchmark_duration", 2);
      config.SetBool("save_reply", true);

      auto query_producer = CreateQueryProducer(TermList{"body"}, 
          config.GetInt("n_threads"));
      auto client = CreateAsyncClient(config, 
                                      std::move(query_producer) 
                                      );
      client->Wait();
      auto reply_pools = client->GetReplyPools();

      for (int i = 0; i < config.GetInt("n_threads"); i++) {
        LOG(INFO) << "reply pool size: " << reply_pools->at(i).size();
      }

      REQUIRE(reply_pools->size() == config.GetInt("n_threads"));
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
  auto server = CreateTestServer(std::string("localhost:50051"), 1, 4, 0);
  utils::sleep(1); // warm up the server

  // Use another thread to create client
  // std::thread client_thread(run_client);

  GeneralConfig config;
  config.SetString("target", "localhost:50051");
  config.SetInt("n_client_channels", 64);
  config.SetInt("n_rpcs_per_channel", 100);
  config.SetInt("n_messages_per_call", 100);
  config.SetInt("n_threads", 8); 
  config.SetInt("n_threads_per_cq", 1);
  config.SetInt("benchmark_duration", 2);
  config.SetBool("save_reply", true);

  auto query_producer = CreateQueryProducer(TermList{"hello"},
      config.GetInt("n_threads"));

  auto client = CreateAsyncClient(config, 
                                  std::move(query_producer));
  client->Wait();
  client.release();
}


TEST_CASE( "Skip list works", "[posting_list]" ) {
  SECTION("10 postings") {
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

    SECTION("Skipforward works") {
      for (int i = 0; i <= 8; i++) {
        REQUIRE(pl.SkipForward(i, 8) == 8);
      }

      for (int i = 0; i < 10; i++) {
        REQUIRE(pl.SkipForward(i, 0) == i);
      }
    }
  }

  SECTION("3 postings, span is 1") {
    PostingList_Vec<PostingSimple> pl("hello", 1);   
    for (int i = 0; i < 3; i++) {
      pl.AddPosting(PostingSimple(i, 88, Positions{28}));
    }

    SECTION("HasSkip()") {
      auto span = pl.GetSkipSpan();
      REQUIRE(span == 1);
    
      std::vector<int> has_skip{0, 1};
      for (auto i : has_skip) {
        REQUIRE(pl.HasSkip(i) == true);
      }

      std::vector<int> not_has_skip{2};
      for (auto i : not_has_skip) {
        REQUIRE(pl.HasSkip(i) == false);
      }
    }

    SECTION("Skipforward works") {
      for (int i = 0; i < 3; i++) {
        REQUIRE(pl.SkipForward(i, 2) == 2);
      }

      for (int i = 0; i < 10; i++) {
        REQUIRE(pl.SkipForward(i, 0) == i);
      }
    }
  }

  SECTION("One postings") {
    PostingList_Vec<PostingSimple> pl("hello", 4);   
    for (int i = 0; i < 1; i++) {
      pl.AddPosting(PostingSimple(i, 88, Positions{28}));
    }

    SECTION("HasSkip()") {
      auto span = pl.GetSkipSpan();
      REQUIRE(span == 4);
    
      std::vector<int> not_has_skip{0};
      for (auto i : not_has_skip) {
        REQUIRE(pl.HasSkip(i) == false);
      }
    }

    SECTION("Skipforward works") {
      for (int i = 0; i < 1; i++) {
        REQUIRE(pl.SkipForward(i, 8) == 1);
      }

      for (int i = 0; i < 1; i++) {
        REQUIRE(pl.SkipForward(i, 0) == i);
      }
    }
  }
}

IteratorPointers create_iterator_pointers(std::vector<PostingListStandardVec> *posting_lists) {
  IteratorPointers pointers;
  for (auto &pl : *posting_lists) {
    pointers.push_back(std::move(pl.Begin()));
  }
  return pointers;
}

// We have to use different ports for this test, otherwise it
// will get segment faults. The faults may be due to the delayed
// cleanup of network status of the OS.
TEST_CASE( "grpc SYNC client and server", "[grpc]" ) {
  GeneralConfig config;
  config.SetString("sync_type", "SYNC");
  config.SetString("engine_name", "qq_mem_compressed");

  SECTION("Start and shutdown") {
    config.SetString("target", "localhost:50054");
    auto server = CreateServer(config);
    server->Shutdown();
    server->Wait();
  }

  SECTION("Serve echo") {
    config.SetString("target", "localhost:50055");
    auto server = CreateServer(config);
    utils::sleep(1);
    auto client = CreateSyncClient("localhost:50055");

    EchoData request;
    request.set_message("hello");
    EchoData reply;
    auto ret = client->Echo(request, reply);

    REQUIRE(ret == true);
    REQUIRE(reply.message() == "hello");

    server->Shutdown();
    server->Wait();
  }

  SECTION("Serve streaming echo") {
    config.SetString("target", "localhost:50056");
    auto server = CreateServer(config);
    utils::sleep(1);
    auto client = CreateSyncClient("localhost:50056");

    EchoData request;
    request.set_message("hello");
    EchoData reply;
    auto ret = client->DoStreamingEcho(request, reply);

    REQUIRE(ret == true);
    REQUIRE(reply.message() == "hello");

    server->Shutdown();
    server->Wait();
  }

  SECTION("Serve search requests") {
    std::vector<int> doc_ids;
    bool ret;

    config.SetString("target", "localhost:50057");
    auto server = CreateServer(config);
    utils::sleep(1);
    auto client = CreateSyncClient("localhost:50057");

    ret = client->Search("body", doc_ids);
    REQUIRE(doc_ids.size() == 0);
    REQUIRE(ret == true);

    client->AddDocument("my title", "my url", "my body");
    client->AddDocument("my title", "my url", "my spirit");

    SearchRequest request;
    SearchQuery query(TermList{"body"});
    query.CopyTo(&request);

    SearchReply reply;
    client->DoStreamingSearch(request, reply);
    
    REQUIRE(reply.entries_size() == 1);
    REQUIRE(reply.entries(0).doc_id() == 0);

    server->Shutdown();
    server->Wait();
  }
}

TEST_CASE( "SyncStreamingClient", "[grpc][sync]" ) {
  GeneralConfig server_config;
  server_config.SetString("sync_type", "SYNC");
  server_config.SetString("engine_name", "qq_mem_compressed");

  GeneralConfig client_config;
  client_config.SetInt("n_threads", 2);
  client_config.SetInt("n_client_channels", 2);
  client_config.SetBool("save_reply", true);
  client_config.SetInt("benchmark_duration", 3);

  std::unique_ptr<QueryProducerService> query_producer = 
    CreateQueryProducer(TermList{"body"}, 2);

  SECTION("SyncStreamingClient") {
    std::string target = "localhost:50061";
    server_config.SetString("target", target);
    auto server = CreateServer(server_config);
    utils::sleep(1);

    auto simple_client = CreateSyncClient(target);
    simple_client->AddDocument("my title", "my url", "my body");
    simple_client->AddDocument("my title", "my url", "my spirit");

    client_config.SetString("target", target);
    SyncStreamingClient client(client_config, 
                               std::move(query_producer));

    auto seconds = client.Wait();
    auto reply_pools = client.GetReplyPools();
    REQUIRE(reply_pools->at(0).size() > 0);
    REQUIRE(reply_pools->at(0)[0].entries(0).doc_id() == 0);
    REQUIRE(reply_pools->at(1).size() > 0);
    REQUIRE(reply_pools->at(1)[0].entries(0).doc_id() == 0);

    client.ShowStats(seconds);

    server->Shutdown();
    server->Wait();
  }

  SECTION("SyncUnaryClient") {
    std::string target = "localhost:50062";
    server_config.SetString("target", target);
    auto server = CreateServer(server_config);
    utils::sleep(1);

    auto simple_client = CreateSyncClient(target);
    simple_client->AddDocument("my title", "my url", "my body");
    simple_client->AddDocument("my title", "my url", "my spirit");

    client_config.SetString("target", target);
    SyncUnaryClient client(client_config, 
                           std::move(query_producer));

    auto seconds = client.Wait();
    auto reply_pools = client.GetReplyPools();
    REQUIRE(reply_pools->at(0).size() > 0);
    REQUIRE(reply_pools->at(0)[0].entries(0).doc_id() == 0);
    REQUIRE(reply_pools->at(1).size() > 0);
    REQUIRE(reply_pools->at(1)[0].entries(0).doc_id() == 0);

    client.ShowStats(seconds);

    server->Shutdown();
    server->Wait();
  }
}

