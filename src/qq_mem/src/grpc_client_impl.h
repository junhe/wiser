#ifndef GRPC_CLIENT_IMPL_H
#define GRPC_CLIENT_IMPL_H

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

#include "histogram.h"
#include "general_config.h"
#include "query_pool.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
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
      const std::string &url, const std::string &body) {
    AddDocumentRequest request;
    request.mutable_document()->set_title(title);
    request.mutable_document()->set_url(url);
    request.mutable_document()->set_body(body);

    request.mutable_options()->set_save(true);

    StatusReply reply;

    ClientContext context;

    // Here we can the stub's newly available method we just added.
    Status status = stub_->AddDocument(&context, request,  &reply);
    return status.ok();
  }

  bool Search(const std::string &term, std::vector<int> &doc_ids) {
    SearchRequest request;
    SearchReply reply;
    ClientContext context;

    assert(doc_ids.size() == 0);

    request.add_terms(term);
    request.set_n_results(10);
    request.set_return_snippets(true);
    request.set_n_snippet_passages(3);
    request.set_query_processing_core(
        qq::SearchRequest_QueryProcessingCore_TOGETHER);

    // Here we can the stub's newly available method we just added.
    Status status = stub_->Search(&context, request,  &reply);

    if (status.ok()) {
      for (int i = 0; i < reply.entries_size(); i++) {
        doc_ids.push_back(reply.entries(i).doc_id());
      }
    }

    return status.ok();
  }

  bool DoStreamingEcho(const EchoData &request, EchoData &reply) {
    ClientContext context;

    std::unique_ptr<ClientReaderWriter<EchoData, EchoData> > stream(stub_->StreamingEcho(&context));
    stream->Write(request);
    stream->WritesDone();
    stream->Read(&reply);
    Status status = stream->Finish();

    if (status.ok()) {
      return true;
    } else {
      std::cout << status.error_code() 
        << ": " << status.error_message()
        << std::endl;
      return false;
    }
  }

  bool DoSyncStreamingSearch(const SearchRequest &request, SearchReply &reply) {
    ClientContext context;

    std::unique_ptr<ClientReaderWriter<SearchRequest, SearchReply> > 
      stream(stub_->SyncStreamingSearch(&context));
    stream->Write(request);
    stream->WritesDone();
    stream->Read(&reply);
    Status status = stream->Finish();

    if (status.ok()) {
      return true;
    } else {
      std::cout << status.error_code() 
        << ": " << status.error_message()
        << std::endl;
      return false;
    }
  }

  bool Echo(const EchoData &request, EchoData &reply) {
    ClientContext context;

    Status status = stub_->Echo(&context, request,  &reply);
    if (status.ok()) {
      return true;
    } else {
      std::cout << "In Echo, " << status.error_code() 
        << ": " << status.error_message()
        << std::endl;
      return false;
    }
  }

 private:
  std::unique_ptr<QQEngine::Stub> stub_;
};


typedef std::vector<qq::SearchReply> ReplyPool;
typedef std::vector<ReplyPool> ReplyPools;
class PerThreadShutdownState;

struct ChannelInfo {
  ChannelInfo(const std::string &target, int shard) {
    grpc::ChannelArguments args;    
    args.SetString("grpc.optimization_target", "throughput");
    args.SetInt("grpc.minimal_stack", 1);
    args.SetInt("shard_to_ensure_no_subchannel_merges", shard);

    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());

    // auto start = utils::now();
    // std::cout << "Waiting for chnnael to be conneted to "
      // << target << std::endl;
    channel_->WaitForConnected(
        gpr_time_add(
          gpr_now(GPR_CLOCK_REALTIME), gpr_time_from_seconds(300, GPR_TIMESPAN)));
    // auto end = utils::now();
    // std::cout << "waited for " << utils::duration(start, end) << std::endl;
    stub_ = QQEngine::NewStub(channel_);
  }

  Channel* get_channel() { return channel_.get(); }
  QQEngine::Stub* get_stub() { return stub_.get(); }

  std::shared_ptr<Channel> channel_;
  std::unique_ptr<QQEngine::Stub> stub_;
};

struct PerThreadShutdownState {
	mutable std::mutex mutex;
	bool shutdown;
	PerThreadShutdownState() : shutdown(false) {}
};

class AsyncClient {
 public:
  AsyncClient() = default;
  AsyncClient(AsyncClient&&) = default;
  AsyncClient& operator=(AsyncClient&&) = default;

  AsyncClient(const GeneralConfig config,
    std::unique_ptr<QueryPoolArray> query_pool_array);
  void DestroyMultithreading();
  void Wait();
  void ShowStats();
  const ReplyPools *GetReplyPools() {
    return &reply_pools_;  
  };
  ~AsyncClient();

  void ThreadFunc(int thread_idx);

 private:
  const GeneralConfig config_;
  std::vector<ChannelInfo> channels_;
  std::vector<std::unique_ptr<CompletionQueue>> cli_cqs_;

  std::vector<std::thread> threads_;
  std::vector<int> cq_; //cq_[thread id]

  std::vector<int> finished_call_counts_;
  std::vector<int> finished_roundtrips_;
  std::vector<std::unique_ptr<PerThreadShutdownState>> shutdown_state_;
  std::vector<Histogram> histograms_;
  std::unique_ptr<QueryPoolArray> query_pool_array_;
  ReplyPools reply_pools_;
  bool save_reply_;
};


class SyncUnaryClient {
 public:
  SyncUnaryClient() = default;
  SyncUnaryClient(SyncUnaryClient&&) = default;
  SyncUnaryClient& operator=(SyncUnaryClient&&) = default;

  SyncUnaryClient(const GeneralConfig config,
      std::unique_ptr<QueryPoolArray> query_pool_array) 
    :config_(config), query_pool_array_(std::move(query_pool_array))
  {
    int num_threads = config.GetInt("n_threads");
    int n_client_channels = config.GetInt("n_client_channels");
    int n_rpcs_per_channel = config.GetInt("n_rpcs_per_channel");

    std::cout << "num_threads: " << num_threads << std::endl;
    std::cout << "n_client_channels: " << n_client_channels << std::endl;
    std::cout << "n_rpcs_per_channel: " << n_rpcs_per_channel << std::endl;

    if (query_pool_array_->Size() != num_threads) {
      throw std::runtime_error(
          "Query pool size is not the same as the number of threads");
    }

    // initialize hitogram vector  
    for (int i = 0; i < num_threads; i++) {
      histograms_.emplace_back();
    }

    // initialize reply pools
    for (int i = 0; i < num_threads; i++) {
      reply_pools_.emplace_back();
    }
    save_reply_ = config.GetBool("save_reply");

    // create channels
    for (int i = 0; i < n_client_channels; i++) {
      channels_.emplace_back(config.GetString("target"), i); 
    }
    std::cout << channels_.size() << " channels created." << std::endl;

    // create threads and counts
    for(int i = 0; i < num_threads; i++)
    {
      threads_.push_back( std::thread(&SyncUnaryClient::ThreadFunc, this, i) );
      finished_call_counts_.push_back(0);
      finished_roundtrips_.push_back(0);
    }
  }
  void DestroyMultithreading() {}
  void Wait() {}
  void ShowStats() {}
  const ReplyPools *GetReplyPools() {
    return &reply_pools_;  
  };
  ~SyncUnaryClient() {}

  void ThreadFunc(int thread_idx) {


  }

 private:
  const GeneralConfig config_;
  std::vector<ChannelInfo> channels_;
  std::vector<std::thread> threads_;

  std::vector<int> finished_call_counts_;
  std::vector<int> finished_roundtrips_;
  std::vector<std::unique_ptr<PerThreadShutdownState>> shutdown_state_;
  std::vector<Histogram> histograms_;
  std::unique_ptr<QueryPoolArray> query_pool_array_;
  ReplyPools reply_pools_;
  bool save_reply_;
};



std::unique_ptr<QQEngineSyncClient> CreateSyncClient(const std::string &target);
std::unique_ptr<AsyncClient> CreateAsyncClient(const GeneralConfig &config,
    std::unique_ptr<QueryPoolArray> query_pool_array);

#endif

