#ifndef QQ_CLIENT_H
#define QQ_CLIENT_H

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpc++/grpc++.h>

#include <grpc/grpc.h>
#include <grpc/support/log.h>
#include <grpc/support/time.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "utils.h"
#include "qq.grpc.pb.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::CompletionQueue;

using qq::QQEngine;
using qq::AddDocumentRequest;
using qq::StatusReply;
using qq::SearchRequest;
using qq::SearchReply;
using qq::EchoData;

// This is a wrapper around the gRPC stub to provide more convenient
// Interface to interact with gRPC server. For example, you can use
// C++ native containers intead of protobuf objects when invoking
// AddDocument().
class QQEngineSyncClient {
    public:
        QQEngineSyncClient(std::shared_ptr<Channel> channel)
            : stub_(QQEngine::NewStub(channel)) {}

        bool AddDocument(const std::string &title, 
                const std::string &url, const std::string &body);
        bool Search(const std::string &term, std::vector<int> &doc_ids);
        bool Echo(const EchoData &request, EchoData &reply);

    private:
        std::unique_ptr<QQEngine::Stub> stub_;
};


typedef std::map<std::string, std::string> ConfigType;
class ChannelInfo;
class PerThreadShutdownState;

class AsyncClient {
 public:
  AsyncClient() = default;
  AsyncClient(AsyncClient&&) = default;
  AsyncClient& operator=(AsyncClient&&) = default;

  AsyncClient(const ConfigType config);
  void DestroyMultithreading();
  void Wait();
  void ShowStats();
  ~AsyncClient();

  void ThreadFunc(int thread_idx);

 private:
  const ConfigType config_;
  std::vector<ChannelInfo> channels_;
  std::vector<std::unique_ptr<CompletionQueue>> cli_cqs_;

  std::vector<std::thread> threads_;
  std::vector<int> cq_; //cq_[thread id]

  std::vector<int> finished_call_counts_;
  std::vector<int> finished_roundtrips_;
  std::vector<std::unique_ptr<PerThreadShutdownState>> shutdown_state_;
};






std::unique_ptr<QQEngineSyncClient> CreateSyncClient(const std::string &target);
std::unique_ptr<AsyncClient> CreateAsyncClient(const std::string &target, 
  int n_client_channels, int n_rpcs_per_channel, int n_messages_per_call,
  int n_async_threads, int n_threads_per_cq, int benchmark_duration);

#endif

