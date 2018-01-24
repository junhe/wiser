#include <signal.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <thread>
#include <string>
#include <cassert>

#include <grpc/grpc.h>
#include <grpc/support/log.h>
#include <grpc/support/time.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>

#include "qq.grpc.pb.h"
#include "grpc_server_impl.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

using qq::SearchRequest;
using qq::SearchReply;
using qq::QQEngine;

using std::chrono::system_clock;


static bool got_sigint = false;


static void sigint_handler(int x) { 
  std::cout << "Caught SIGINT" << std::endl;
  got_sigint = true; 
}



int main(int argc, char** argv) {
  if (argc != 6) {
    std::cout << "Usage: exefile port secs threads n_docs sync_type" << std::endl;
    exit(1);
  }

  signal(SIGINT, sigint_handler);

  std::string port(argv[1]);
  int n_secs = std::stoi(argv[2]);
  const int n_threads = std::atoi(argv[3]);
  const int n_docs = std::atoi(argv[4]);
  auto sync_type = argv[5];

  GeneralConfig config;
  config.SetString("target", std::string("localhost:") + port);
  config.SetString("sync_type", sync_type);
  config.SetInt("n_line_doc_rows", n_docs);
  config.SetString("line_doc_path", 
      "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc");

  if (config.GetString("sync_type") == "ASYNC") {
    config.SetInt("n_server_threads", n_threads);
    config.SetInt("server_duration", n_secs);
    config.SetInt("n_threads_per_cq", 1);
  } else {
    // nothing to add
  }

  auto server = CreateServer(config);

  if (n_secs == 0) {
    while (!got_sigint) {
      gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                   gpr_time_from_seconds(5, GPR_TIMESPAN)));
    }
  } else {
    server->Wait();
  }

  std::cout << "Server about to be destruct" << std::endl;
  return 0;
}
