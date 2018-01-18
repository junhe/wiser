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
    utils::sleep(1);

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

    doc_ids.clear();
    client->Search("body", doc_ids);
    REQUIRE(ret == true);
    REQUIRE(doc_ids.size() == 1);

    // server will automatically destructed here.
  }

}


