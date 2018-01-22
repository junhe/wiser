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

TEST_CASE( "QueryProcessor works", "[engine]" ) {
  OffsetPairs offset_pairs;
  for (int i = 0; i < 10; i++) {
    offset_pairs.push_back(std::make_tuple(1, 2)); 
  }

  PostingList_Vec<PostingWO> pl01("hello");
  PostingList_Vec<PostingWO> pl02("world");
  PostingList_Vec<PostingWO> pl03("again");

  for (int i = 0; i < 5; i++) {
    pl01.AddPosting(PostingWO(i, 3, offset_pairs));
    pl02.AddPosting(PostingWO(i, 3, offset_pairs));
    pl03.AddPosting(PostingWO(i, 3, offset_pairs));
  }


  // Setting doc length this will give doc 4 the highest score 
  // because the document is the shortest.
  DocLengthStore store;
  for (int i = 0; i < 5; i++) {
    store.AddLength(i, (5 - i) * 10);
  }

  SECTION("Find top 5") {
    const std::vector<const PostingList_Vec<PostingWO>*> lists{&pl01, &pl02};

    QueryProcessor<PostingWO> processor(lists, store, 100, 5);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 5);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3, 2, 1, 0});
  }

  SECTION("Find top 2") {
    const std::vector<const PostingList_Vec<PostingWO>*> lists{&pl01, &pl02};

    QueryProcessor<PostingWO> processor(lists, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 within one postinglist") {
    const std::vector<const PostingList_Vec<PostingWO>*> lists{&pl01};

    QueryProcessor<PostingWO> processor(lists, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

  SECTION("Find top 2 with three posting lists") {
    const std::vector<const PostingList_Vec<PostingWO>*> lists{&pl01, &pl02, &pl03};

    QueryProcessor<PostingWO> processor(lists, store, 100, 2);
    std::vector<ResultDocEntry> result = processor.Process();
    REQUIRE(result.size() == 2);

    std::vector<DocIdType> doc_ids;
    for (auto & entry : result) {
      doc_ids.push_back(entry.doc_id);
    }

    REQUIRE(doc_ids == std::vector<DocIdType>{4, 3});
  }

}


TEST_CASE( "grpc SYNC client and server", "[grpc0]" ) {
  GeneralConfig config;
  config.SetString("server_type", "SYNC");
  config.SetString("target", "localhost:50051");

  SECTION("Start and shutdown") {
    auto server = CreateServer(config);
    server->Shutdown();
    server->Wait();
  }

  SECTION("Serve echo") {
    auto server = CreateServer(config);
    utils::sleep(1);
    auto client = CreateSyncClient("localhost:50051");

    EchoData request;
    request.set_message("hello");
    EchoData reply;
    auto ret = client->Echo(request, reply);

    REQUIRE(ret == true);
    REQUIRE(reply.message() == "hello");

    server->Shutdown();
    server->Wait();
  }

}


