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

#include "grpc_service_impl.h"

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


void sleep(int n_secs) {
  using namespace std::this_thread; // sleep_for, sleep_until
  using namespace std::chrono; // nanoseconds, system_clock, seconds

  sleep_for(seconds(n_secs));
}

std::mutex w_mutex;
int write_count = 0;

class AsyncServer {
 public:
  AsyncServer(const ConfigType &config): config_(config) {
    ServerBuilder builder;

    std::cout << "listening on " << config.at("target") << std::endl;
    builder.AddListeningPort(config.at("target"), grpc::InsecureServerCredentials());
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
        contexts_.emplace_back(new ServerRpcContext(&async_service_, srv_cqs_[j].get()));
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
    std::cout << "Server write count " << write_count << std::endl;

  }

  void Wait() {
    sleep(std::stoi(config_.at("server_duration")));
  }


 private:
  class ServerRpcContext;

  void ThreadFunc(int thread_idx) {
    // Wait until work is available or we are shutting down
    bool ok;
    void *got_tag;
    std::cout << "in ThreadFunc" << std::endl;
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
      ServerRpcContext(QQEngine::AsyncService *async_service,
                       grpc::ServerCompletionQueue *cq
                       )
        : async_service_(async_service), 
          cq_(cq),
          srv_ctx_(new grpc::ServerContext),
          next_state_(State::REQUEST_DONE),
          stream_(srv_ctx_.get())
      {
          response_.add_doc_ids(88);
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
            // std::cout << "State REQUEST_DONE" << std::endl;
            if (!ok) {
              return false;
            }
            next_state_ = State::READ_DONE;
            stream_.Read(&req_, AsyncServer::tag(this));
            return true;
          case State::READ_DONE:
            // std::cout << "State READ_DONE" << std::endl;
            if (ok) {
              next_state_ = State::WRITE_DONE;
              stream_.Write(response_, AsyncServer::tag(this));
              w_mutex.lock();
              write_count++;
              w_mutex.unlock();
            } else {  // client has sent writes done
              next_state_ = State::FINISH_DONE;
              stream_.Finish(Status::OK, AsyncServer::tag(this));
            }
            return true;
          case State::WRITE_DONE:
            // std::cout << "State WRITE_DONE" << std::endl;
            if (ok) {
              next_state_ = State::READ_DONE;
              stream_.Read(&req_, AsyncServer::tag(this));
            } else {
              next_state_ = State::FINISH_DONE;
              stream_.Finish(Status::OK, AsyncServer::tag(this));
            }
            return true;
          case State::FINISH_DONE:
            // std::cout << "State FINISH_DONE" << std::endl;
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
    QQEngine::AsyncService *async_service_;
    std::unique_ptr<grpc::ServerContext> srv_ctx_;
    SearchRequest req_;
    SearchReply response_;
    // std::unique_ptr<grpc::ServerAsyncReaderWriter<SearchReply, SearchRequest>> stream_;
    grpc::ServerAsyncReaderWriter<SearchReply, SearchRequest> stream_;
    std::mutex mu_;
    grpc::Status status_;
  };

  std::vector<std::thread> threads_;
  std::unique_ptr<grpc::Server> server_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> srv_cqs_;
  std::vector<int> cq_;
  QQEngine::AsyncService async_service_;
  std::vector<std::unique_ptr<ServerRpcContext>> contexts_;

  struct PerThreadShutdownState {
    mutable std::mutex mutex;
    bool shutdown;
    PerThreadShutdownState() : shutdown(false) {}
  };

  std::vector<std::unique_ptr<PerThreadShutdownState>> shutdown_state_;
  const ConfigType &config_;
};


