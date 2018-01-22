/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#ifndef GRPC_SERVER_IMPL_H
#define GRPC_SERVER_IMPL_H

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

#include "types.h"
#include "qq_engine.h"
#include "qq_mem_uncompressed_engine.h"
#include "engine_loader.h"
#include "utils.h"
#include "general_config.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

using qq::QQEngine;

// for AddDocument
using qq::AddDocumentRequest;
using qq::StatusReply;

// for Search
using qq::SearchRequest;
using qq::SearchReply;
using qq::SearchReplyEntry;

// for Echo
using qq::EchoData;



using std::chrono::system_clock;


typedef std::map<std::string, std::string> ConfigType;


// Service Inheritence:
// Service
//   |
// WithAsyncMethod_StreamingSearch: Async methods are provided here
//   |
// QQEngineServiceImpl: Sync methods are implemented here
class QQEngineServiceImpl: public QQEngine::WithAsyncMethod_StreamingSearch<QQEngine::Service> {
  public:
    void Initialize(SearchEngineServiceNew* search_engine) {
      // TODO: This function is ugly. Need to find a better way to 
      // initialize search engine pointer at class initialization.
      search_engine_ = search_engine;
    }

    Status AddDocument(ServerContext* context, const AddDocumentRequest* request,
            StatusReply* reply) override {
        // In this case, body must be already tokenized.
        search_engine_->AddDocument(request->document().body(), 
            request->document().body());

        count++;

        if (count % 10000 == 0) {
            std::cout << count << std::endl;
        }

        reply->set_message("Doc added");
        reply->set_ok(true);
        return Status::OK;
    }

    Status Search(ServerContext* context, const SearchRequest* request,
            SearchReply* reply) override {
        SearchResult result = search_engine_->Search(SearchQuery(*request));
        result.CopyTo(reply);

        return Status::OK;
    }

    Status Echo(ServerContext* context, const EchoData* request,
            EchoData* reply) override {

        std::string msg = request->message();
        reply->set_message(msg);
        
        return Status::OK;
    }

    private:
        int count = 0;
        SearchEngineServiceNew* search_engine_ = nullptr;
};



class AsyncServer {
 public:
  AsyncServer(const ConfigType config, std::unique_ptr<SearchEngineServiceNew> engine)
      :config_(config), search_engine_(std::move(engine)) {

    std::string line_doc_path = config.at("line_doc_path");
    if (line_doc_path.size() > 0) {
      int n_rows = std::stoi(config.at("n_line_doc_rows"));
      // int ret = search_engine_->LoadLocalDocuments(line_doc_path, n_rows);
      int ret = engine_loader::load_body_and_tokenized_body(search_engine_.get(), 
                                                            line_doc_path, n_rows, 2, 2);
      // std::cout << ret << " docs indexed to search engine." << std::endl;
    }

    ServerBuilder builder;

    std::cout << "listening on " << config.at("target") << std::endl;
    builder.AddListeningPort(config.at("target"), grpc::InsecureServerCredentials());
    async_service_.Initialize(search_engine_.get());
    builder.RegisterService(&async_service_);
    
    int num_threads = std::stoi(config.at("n_server_threads")); 
    int tpc = std::stoi(config.at("n_threads_per_cq"));  // 1 if unspecified
    int num_cqs = (num_threads + tpc - 1) / tpc;     // ceiling operator
    for (int i = 0; i < num_cqs; i++) {
      srv_cqs_.emplace_back(builder.AddCompletionQueue());
    }
    for (int i = 0; i < num_threads; i++) {
      cq_.emplace_back(i % srv_cqs_.size());
    }

    builder.AddChannelArgument("grpc.optimization_target", "throughput");
    builder.AddChannelArgument("prpc.minimal_stack", 1);

    server_ = builder.BuildAndStart();

    // contexts_ has [cq0, cq1, ... cqn-1, cq0, .....]
    for (int i = 0; i < 5000; i++) {
      for (int j = 0; j < num_cqs; j++) {
        contexts_.emplace_back(new ServerRpcContext(
              &async_service_, srv_cqs_[j].get(), search_engine_.get()));
      }
    }
    assert(contexts_.size() == 5000 * num_cqs);

    for (int i = 0; i < num_threads; i++) {
      shutdown_state_.emplace_back(new PerThreadShutdownState());
      threads_.emplace_back(&AsyncServer::ThreadFunc, this, i);
    }

  }

  ~AsyncServer() {
    std::cout << "Cleaning up" << std::endl;

    for (auto ss = shutdown_state_.begin(); ss != shutdown_state_.end(); ++ss) {
      std::lock_guard<std::mutex> lock((*ss)->mutex);
      (*ss)->shutdown = true;
    }

    std::thread shutdown_thread(&AsyncServer::ShutdownThreadFunc, this);

    for (auto cq = srv_cqs_.begin(); cq != srv_cqs_.end(); ++cq) {
      (*cq)->Shutdown();
    }
    for (auto thr = threads_.begin(); thr != threads_.end(); thr++) {
      thr->join();
    }

    // Clean up cq
    for (auto cq = srv_cqs_.begin(); cq != srv_cqs_.end(); ++cq) {
      bool ok;
      void *got_tag;
      while ((*cq)->Next(&got_tag, &ok))
        ;
    }

    shutdown_thread.join();
    std::cout << "Server destructed" << std::endl;
  }

  void Wait() {
    utils::sleep(std::stoi(config_.at("server_duration")));
  }


 private:
  class ServerRpcContext;

  void ThreadFunc(int thread_idx) {
    // Wait until work is available or we are shutting down
    bool ok;
    void *got_tag;
    while (srv_cqs_[cq_[thread_idx]]->Next(&got_tag, &ok)) {
      ServerRpcContext *ctx = detag(got_tag);
      
      std::lock_guard<std::mutex> l(shutdown_state_[thread_idx]->mutex);
      if (shutdown_state_[thread_idx]->shutdown) {
        return;
      }
      // std::lock_guard<ServerRpcContext> l2(*ctx);
      const bool still_going = ctx->RunNextState(ok);
      // if this RPC context is done, refresh it
      if (!still_going) {
        ctx->Reset();
      }
    }
    return;
  }

  void ShutdownThreadFunc() {
    // TODO (vpai): Remove this deadline and allow Shutdown to finish properly
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(3);
    server_->Shutdown(deadline);
  }

  static void *tag(ServerRpcContext *c) {
    return reinterpret_cast<void *>(c);
  }

  static ServerRpcContext *detag(void *tag) {
    return reinterpret_cast<ServerRpcContext *>(tag);
  }

  class ServerRpcContext {
   public:
      ServerRpcContext(QQEngineServiceImpl *async_service,
                       grpc::ServerCompletionQueue *cq,
                       SearchEngineServiceNew *search_engine)
        : async_service_(async_service), 
          cq_(cq),
          srv_ctx_(new grpc::ServerContext),
          next_state_(State::REQUEST_DONE),
          stream_(srv_ctx_.get()),
          search_engine_(search_engine)
      {
          RequestCall();
      }

      void Reset() {
        srv_ctx_.reset(new grpc::ServerContext);
        req_ = SearchRequest();
        stream_ = grpc::ServerAsyncReaderWriter<SearchReply, SearchRequest>(
            srv_ctx_.get());

        next_state_ = State::REQUEST_DONE;
        RequestCall();
      }

      bool RunNextState(bool ok) {
        switch(next_state_) {
          case State::REQUEST_DONE:
            if (!ok) {
              return false;
            }
            next_state_ = State::READ_DONE;
            stream_.Read(&req_, AsyncServer::tag(this));
            return true;
          case State::READ_DONE:
            if (ok) {
              next_state_ = State::WRITE_DONE;

              auto result = search_engine_->Search(SearchQuery(req_));
              result.CopyTo(&response_);

              stream_.Write(response_, AsyncServer::tag(this));
            } else {  // client has sent writes done
              next_state_ = State::FINISH_DONE;
              stream_.Finish(Status::OK, AsyncServer::tag(this));
            }
            return true;
          case State::WRITE_DONE:
            if (ok) {
              next_state_ = State::READ_DONE;
              stream_.Read(&req_, AsyncServer::tag(this));
            } else {
              next_state_ = State::FINISH_DONE;
              stream_.Finish(Status::OK, AsyncServer::tag(this));
            }
            return true;
          case State::FINISH_DONE:
            return false;
          default:
            GPR_ASSERT(false);
            return false;
        }
      }

      void lock() { mu_.lock(); }
      void unlock() { mu_.unlock(); }

   private:
    void RequestCall() {
      async_service_->RequestStreamingSearch(srv_ctx_.get(), &stream_, cq_, cq_, 
          AsyncServer::tag(this));
    }


    enum State {
      REQUEST_DONE,
      READ_DONE,
      WRITE_DONE,
      FINISH_DONE
    };

    State next_state_;
    grpc::ServerCompletionQueue *cq_;
    QQEngineServiceImpl *async_service_;
    std::unique_ptr<grpc::ServerContext> srv_ctx_;
    SearchRequest req_;
    SearchReply response_;
    // std::unique_ptr<grpc::ServerAsyncReaderWriter<SearchReply, SearchRequest>> stream_;
    grpc::ServerAsyncReaderWriter<SearchReply, SearchRequest> stream_;
    std::mutex mu_;
    grpc::Status status_;
    SearchEngineServiceNew *search_engine_;
  };

  std::vector<std::thread> threads_;
  std::unique_ptr<grpc::Server> server_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> srv_cqs_;
  std::vector<int> cq_;
  QQEngineServiceImpl async_service_;
  std::vector<std::unique_ptr<ServerRpcContext>> contexts_;

  struct PerThreadShutdownState {
    mutable std::mutex mutex;
    bool shutdown;
    PerThreadShutdownState() : shutdown(false) {}
  };

  std::vector<std::unique_ptr<PerThreadShutdownState>> shutdown_state_;
  const ConfigType &config_;
  std::unique_ptr<SearchEngineServiceNew> search_engine_;
};



class SyncServer {
 public:
  SyncServer(const GeneralConfig config, std::unique_ptr<SearchEngineServiceNew> engine)
      :config_(config), search_engine_(std::move(engine)) {

    const std::string target = config.GetString("target");

    ServerBuilder builder;
    builder.AddListeningPort(target, grpc::InsecureServerCredentials());

    sync_service_.Initialize(search_engine_.get());

    builder.RegisterService(&sync_service_);
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << target << std::endl;
  }

  void Wait() {
    server_->Wait();
  }

 private:
  const GeneralConfig config_;
  std::unique_ptr<SearchEngineServiceNew> search_engine_;
  QQEngineServiceImpl sync_service_;
  std::unique_ptr<grpc::Server> server_;
};



std::unique_ptr<AsyncServer> CreateServer(const std::string &target, 
    int n_threads_per_cq, int n_server_threads, int n_secs);
std::unique_ptr<AsyncServer> CreateServer(const std::string &target, 
    int n_threads_per_cq, int n_server_threads, int n_secs,
    const std::string &line_doc_path, const int &n_rows);

#endif
