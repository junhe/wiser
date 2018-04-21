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


DEFINE_string(engine, "missing", 
    "[qq_mem_compressed / vacuum:vacuum_dump:/mnt/ssd/vacuum_engine_dump_magic");
DEFINE_string(addr, "localhost", "GRPC listening address.");
DEFINE_string(port, "50051", "GRPC listening port.");
DEFINE_int32(n_secs, 0, "Server running time (seconds).");
DEFINE_int32(n_threads, 1, "Number of async GRPC threads (it has no effect in SYNC mode.");
DEFINE_int32(n_docs, 1000, "Number of documents to load");
DEFINE_string(sync_type, "ASYNC", "Type of GPRC sync [ASYNC/SYNC]");



static bool got_sigint = false;


static void sigint_handler(int x) { 
  std::cout << "Caught SIGINT" << std::endl;
  got_sigint = true; 
}



int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

  gflags::SetUsageMessage("Usage: " + std::string(argv[0]));
	gflags::ParseCommandLineFlags(&argc, &argv, true);

  signal(SIGINT, sigint_handler);

  int n_secs = FLAGS_n_secs;
  const int n_threads = FLAGS_n_threads;
  const int n_docs = FLAGS_n_docs;
  auto sync_type = FLAGS_sync_type;

  GeneralConfig config;
  // config.SetString("target", std::string("node.conan-wisc.fsperfatscale-pg0:") + FLAGS_port);
  config.SetString("target", FLAGS_addr + ":" + FLAGS_port);
  // config.SetString("engine_name", "qq_mem_compressed");
  if (FLAGS_engine == "missing") 
    LOG(FATAL) << "Must set engine!";
  config.SetString("engine_name", FLAGS_engine);
  config.SetString("sync_type", sync_type);

  config.SetString("load_source", "dump");
  config.SetString("dump_path", "/mnt/ssd/big-engine-char-length-04-19");

  config.SetInt("n_line_doc_rows", n_docs);
  config.SetString("line_doc_format", "WITH_POSITIONS");
  config.SetString("line_doc_path", 
      "/mnt/ssd/downloads/enwiki.linedoc_tokenized.1");
  
  // config.SetString("line_doc_format", "TOKEN_ONLY");
  // config.SetString("line_doc_path", 
      // "/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc");

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
