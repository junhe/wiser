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



typedef std::map<std::string, std::string> ConfigType;

static bool got_sigint = false;


static void sigint_handler(int x) { 
  std::cout << "Caught SIGINT" << std::endl;
  got_sigint = true; 
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Usage: exefile port secs" << std::endl;
    exit(1);
  }

  signal(SIGINT, sigint_handler);

  std::string port(argv[1]);
  std::string n_secs(argv[2]);

  // ConfigType config;
  // config["target"] = std::string("localhost:") + port;
  // config["n_threads_per_cq"] = "1";
  // config["n_server_threads"] = "40";
  // config["server_duration"] = n_secs;

  // AsyncServer server(config);
  auto server = CreateServer(std::string("localhost:") + port, 1, 40, 0);

  if (std::stoi(n_secs) == 0) {
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
