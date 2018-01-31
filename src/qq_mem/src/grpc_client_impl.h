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


#define HISTOGRAM_TIME_SCALE 1e6

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


class RPCContext {
 public:
  RPCContext(CompletionQueue* cq, QQEngine::Stub* stub, 
             int n_messages_per_call, std::vector<int> &finished_call_counts,
             std::vector<int> &finished_roundtrips)
    :cq_(cq), stub_(stub), next_state_(State::INITIALIZED), 
    n_messages_per_call_(n_messages_per_call), 
    n_issued_(0),
    finished_call_counts_(finished_call_counts),
    finished_roundtrips_(finished_roundtrips)
  {
      req_.add_terms("hello");
      Start();
  } 

  void Start() {
    // get a stream, start call, init state
    stream_ = stub_->PrepareAsyncStreamingSearch(&context_, cq_);

    // this must go before StartCall(), otherwise another thread
    // may get an event about this context and call RunNextState()
    // when next_state_ is still State::INITIALIZED
    next_state_ = State::STREAM_IDLE; 

    stream_->StartCall(RPCContext::tag(this));
  }

  void TryCancel() {
    context_.TryCancel();
  }

  bool RunNextState(bool ok, int thread_idx, Histogram *hist, 
      QueryPool *query_pool, ReplyPool *reply_pool, bool save_reply) {
    TermList terms;
    int i;

    while (true) {
      switch (next_state_) {
        case State::STREAM_IDLE:
          next_state_ = State::READY_TO_WRITE;
          break;  // loop around, so we proceed to writing.
        case State::WAIT:
          // We do not wait in this client
          GPR_ASSERT(false);
        case State::READY_TO_WRITE:
          if (!ok) {
            return false;
          }
          
          req_.clear_terms();
          req_.set_n_results(10);
          terms = query_pool->Next();    
          for (i = 0; i < terms.size(); i++) {
            req_.add_terms(terms[i]);
          }

          start_ = utils::now();
          next_state_ = State::WRITE_DONE;
          stream_->Write(req_, RPCContext::tag(this));
          return true;
        case State::WRITE_DONE:
          if (!ok) {
            return false;
          }
          next_state_ = State::READ_DONE;
          stream_->Read(&reply_, RPCContext::tag(this));
          return true;
        case State::READ_DONE:
          ++n_issued_;
          finished_roundtrips_[thread_idx]++;

          if (save_reply == true) {
            reply_pool->push_back(reply_);
          }

          end_ = utils::now();
          duration_ = utils::duration(start_, end_);
          hist->Add(duration_ * HISTOGRAM_TIME_SCALE);

          if (n_issued_ < n_messages_per_call_) {
            // has more to do
            next_state_ = State::STREAM_IDLE;
            break; // loop around to the next state immediately
          } else {
            next_state_ = State::WRITES_DONE_DONE;
            stream_->WritesDone(RPCContext::tag(this));
            return true;
          }
        case State::WRITES_DONE_DONE:
          next_state_ = State::FINISH_DONE;
          stream_->Finish(&status_, RPCContext::tag(this));
          return true;
        case State::FINISH_DONE:
          finished_call_counts_[thread_idx]++;
          next_state_ = State::FINISH_DONE_DONE;
          return false;
        default:
          std::cout << "State: INITIALIZED " 
            << (next_state_ == State::INITIALIZED) << std::endl;
          std::cout << "State: FINISH_DONE_DONE " 
            << (next_state_ == State::FINISH_DONE_DONE) << std::endl;
          GPR_ASSERT(false);
          return false;
      }
    }
    return true;
  }

  void StartNewClone() {
    auto* clone = new RPCContext(cq_, stub_, n_messages_per_call_, 
                                 finished_call_counts_, finished_roundtrips_);
    // clone->Start();
  }

  static void* tag(RPCContext *c) { return reinterpret_cast<void*>(c); }
  static RPCContext* detag(void* t) {
    return reinterpret_cast<RPCContext*>(t);
  }

 private:
  enum State {
    INVALID,
    STREAM_IDLE,
    WAIT,
    READY_TO_WRITE,
    WRITE_DONE,
    READ_DONE,
    WRITES_DONE_DONE,
    FINISH_DONE,
    INITIALIZED,
    FINISH_DONE_DONE
  };

  int n_messages_per_call_; 
  int n_issued_;
  QQEngine::Stub* stub_;
  ClientContext context_;
  CompletionQueue* cq_;
  SearchRequest req_;
  SearchReply reply_;
  State next_state_;
  Status status_;
  std::unique_ptr<grpc::ClientAsyncReaderWriter<SearchRequest, SearchReply>> stream_;
  std::vector<int> &finished_call_counts_;
  std::vector<int> &finished_roundtrips_;
  utils::time_point start_, end_;
  double duration_;
};





class Client {
 public:
  Client() = default;
  Client(Client&&) = default;
  Client& operator=(Client&&) = default;

  // Must set the following items in config:
  //  n_threads
  //  n_client_channels
  //  save_reply
  //  target
  //  benchmark_duration
  Client(const GeneralConfig config,
      std::unique_ptr<QueryPoolArray> query_pool_array) 
    :config_(config), query_pool_array_(std::move(query_pool_array))
  {
    int num_threads = config.GetInt("n_threads");
    int n_client_channels = config.GetInt("n_client_channels");

    std::cout << "num_threads: " << num_threads << std::endl;
    std::cout << "n_client_channels: " << n_client_channels << std::endl;

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

    for (int i = 0; i < num_threads; i++) {
      shutdown_state_.emplace_back(new PerThreadShutdownState());
    }

    for (int i = 0; i < num_threads; i++) {
      finished_call_counts_.push_back(0);
      finished_roundtrips_.push_back(0);
    }

    // create channels
    for (int i = 0; i < n_client_channels; i++) {
      channels_.emplace_back(config.GetString("target"), i); 
    }
    std::cout << channels_.size() << " channels created." << std::endl;

    // create threads and counts
    for(int i = 0; i < num_threads; i++)
    {
      finished_call_counts_.push_back(0);
      finished_roundtrips_.push_back(0);
    }
  }

  void StartThreads() {
    int num_threads = config_.GetInt("n_threads");
    for(int i = 0; i < num_threads; i++)
    {
      threads_.push_back( std::thread(&Client::ThreadFunc, this, i) );
    }
  }

  virtual void DestroyMultithreading() {
    std::cout << "Destroying threads..." << std::endl;
    for (auto ss = shutdown_state_.begin(); ss != shutdown_state_.end(); ++ss) {
      std::lock_guard<std::mutex> lock((*ss)->mutex);
      (*ss)->shutdown = true;
    }
    std::cout << "Threads are destroyed." << std::endl;
  }

  virtual void ThreadFunc(int thread_idx) = 0;

  void Wait() {
    std::cout << "Waiting for " << config_.GetInt("benchmark_duration") << " seconds.\n";
    utils::sleep(config_.GetInt("benchmark_duration"));
    std::cout << "The wait is done" << std::endl;

    DestroyMultithreading();

    std::for_each(threads_.begin(), threads_.end(), std::mem_fn(&std::thread::join));
  }

  void ShowStats() {
    std::cout << "Finished round trips: " << GetTotalRoundtrips() << std::endl;

    auto n_secs = config_.GetInt("benchmark_duration");
    std::cout << "Duration: " << n_secs << std::endl;
    std::cout << "Total roundtrips: " << GetTotalRoundtrips() << std::endl;
    std::cout << "Roundtrip Per Second: " 
      << GetTotalRoundtrips() / n_secs << std::endl;

    Histogram hist_all;
    for (auto & histogram : histograms_) {
      hist_all.Merge(histogram);
    }

    std::cout << "---- Latency histogram (us) ----" << std::endl;
    std::vector<int> percentiles{0, 25, 50, 75, 90, 95, 99, 100};
    for (auto percentile : percentiles) {
      std::cout << "Percentile " << std::setw(4) << percentile << ": " 
        << std::setw(25) 
        << utils::format_with_commas<int>(round(hist_all.Percentile(percentile))) 
        << std::endl;
    }

    if (save_reply_ == true) {
      std::cout << "---- Reply Pool sizes ----" << std::endl;
      for (auto &pool : reply_pools_) {
        std::cout << pool.size() << " ";
      }
      std::cout << std::endl;
    }
  }
  const ReplyPools *GetReplyPools() {
    return &reply_pools_;  
  };
  ~Client() {}

  int GetTotalRoundtrips() {
    int total_roundtrips = 0;
    for (auto count : finished_roundtrips_) {
      total_roundtrips += count;
    }
    return total_roundtrips;
  }

 protected:
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


class SyncStreamingClient: public Client {
 public:
  SyncStreamingClient() = default;
  SyncStreamingClient(SyncStreamingClient&&) = default;
  SyncStreamingClient& operator=(SyncStreamingClient&&) = default;

  // Must set the following items in config:
  //  n_threads
  //  n_client_channels
  //  save_reply
  //  target
  //  benchmark_duration
  SyncStreamingClient(const GeneralConfig config,
      std::unique_ptr<QueryPoolArray> query_pool_array) 
    :Client(config, std::move(query_pool_array))
  {
    StartThreads();
  }

  void StreamingSearch(const int thread_idx,
                       ClientReaderWriter<SearchRequest, SearchReply> *stream,
                       const SearchRequest &request, SearchReply &reply) {

    auto start = utils::now();
    stream->Write(request);
    stream->Read(&reply);
    auto end = utils::now();
    auto duration = utils::duration(start, end);

    histograms_[thread_idx].Add(duration * HISTOGRAM_TIME_SCALE);
    finished_roundtrips_[thread_idx]++;
  }

  void ThreadFunc(int thread_idx) {
    TermList terms;
    SearchRequest grpc_request;
    grpc_request.set_n_results(10);
    grpc_request.set_return_snippets(true);
    grpc_request.set_n_snippet_passages(3);
    SearchReply reply;

    ClientContext context;
    auto stub = channels_[thread_idx % channels_.size()].get_stub();
    std::unique_ptr<ClientReaderWriter<SearchRequest, SearchReply> > stream(
        stub->SyncStreamingSearch(&context));

    while (shutdown_state_[thread_idx]->shutdown == false) {
      terms = query_pool_array_->Next(thread_idx);
      grpc_request.clear_terms();
      for (int i = 0; i < terms.size(); i++) {
        grpc_request.add_terms(terms[i]);
      }

      StreamingSearch(thread_idx, stream.get(), grpc_request, reply);

      if (save_reply_) {
        reply_pools_[thread_idx].push_back(reply);
      }
    }
    // context.TryCancel();
    stream->WritesDone();
    stream->Finish();
  }
};



class AsyncClient: public Client {
 public:
  AsyncClient() = default;
  AsyncClient(AsyncClient&&) = default;
  AsyncClient& operator=(AsyncClient&&) = default;

  AsyncClient(const GeneralConfig config,
    std::unique_ptr<QueryPoolArray> query_pool_array)
    :Client(config, std::move(query_pool_array))
  {
    int tpc = config.GetInt("n_threads_per_cq");
    int num_async_threads = config.GetInt("n_threads");
    int n_client_channels = config.GetInt("n_client_channels");
    int n_rpcs_per_channel = config.GetInt("n_rpcs_per_channel");

    std::cout << "n_threads_per_cq: " << tpc << std::endl;
    std::cout << "num_async_threads: " << num_async_threads << std::endl;
    std::cout << "n_client_channels: " << n_client_channels << std::endl;
    std::cout << "n_rpcs_per_channel: " << n_rpcs_per_channel << std::endl;

    // Initialize completion queues
    int num_cqs = (num_async_threads + tpc - 1) / tpc;  // ceiling operator
    for (int i = 0; i < num_cqs; i++) {
      std::cout << "Creating CQ " << i << std::endl;
      cli_cqs_.emplace_back(new CompletionQueue);
    }

    // completion queue index for a thread
    for (int i = 0; i < num_async_threads; i++) {
      cq_.emplace_back(i % cli_cqs_.size());
    }
    
    int t = 0;
    for (int ch = 0; ch < n_client_channels; ch++) {
      for (int i = 0; i < n_rpcs_per_channel; i++) {
        auto* cq = cli_cqs_[t].get();
        auto ctx = new RPCContext(
            cq, 
            channels_[ch].get_stub(),
            config.GetInt("n_messages_per_call"), 
            finished_call_counts_,
            finished_roundtrips_);
      }
      t = (t + 1) % cli_cqs_.size();
    }

    StartThreads();
  }

  void DestroyMultithreading() {
    for (auto ss = shutdown_state_.begin(); ss != shutdown_state_.end(); ++ss) {
      std::lock_guard<std::mutex> lock((*ss)->mutex);
      (*ss)->shutdown = true;
    }

    for (auto cq = cli_cqs_.begin(); cq != cli_cqs_.end(); cq++) {
      (*cq)->Shutdown();
    }
  }

  ~AsyncClient() {
    for (auto cq = cli_cqs_.begin(); cq != cli_cqs_.end(); cq++) {
      void* got_tag;
      bool ok;
      while ((*cq)->Next(&got_tag, &ok)) {
        delete RPCContext::detag(got_tag);
      }
    }
  }

  void ThreadFunc(int thread_idx) {
    void* got_tag;
    bool ok;

    while (cli_cqs_[cq_[thread_idx]]->Next(&got_tag, &ok)) {
      RPCContext* ctx = RPCContext::detag(got_tag);

      std::lock_guard<std::mutex> l(shutdown_state_[thread_idx]->mutex);
      if (shutdown_state_[thread_idx]->shutdown == true) {
        ctx->TryCancel();
        delete ctx;
        break;
      }

      if (!ctx->RunNextState(ok, thread_idx, &histograms_[thread_idx], 
            query_pool_array_->GetPool(thread_idx),
            &reply_pools_[thread_idx], save_reply_)) 
      {
        ctx->StartNewClone();
        delete ctx;
      } 
    }

    std::cout << "Thread " << thread_idx << " ends" << std::endl;
  }

 private:
  std::vector<std::unique_ptr<CompletionQueue>> cli_cqs_;
  std::vector<int> cq_; //cq_[thread id]
};


class SyncUnaryClient: public Client {
 public:
  SyncUnaryClient() = default;
  SyncUnaryClient(SyncUnaryClient&&) = default;
  SyncUnaryClient& operator=(SyncUnaryClient&&) = default;

  // Must set the following items in config:
  //  n_threads
  //  n_client_channels
  //  save_reply
  //  target
  //  benchmark_duration
  SyncUnaryClient(const GeneralConfig config,
      std::unique_ptr<QueryPoolArray> query_pool_array) 
    :Client(config, std::move(query_pool_array))
  {
    StartThreads();
  }

  void UnarySearch(const int thread_idx, const SearchRequest &request, SearchReply &reply) {
    ClientContext context;
    auto stub = channels_[thread_idx % channels_.size()].get_stub();

    auto start = utils::now();
    Status status = stub->Search(&context, request,  &reply);
    auto end = utils::now();
    auto duration = utils::duration(start, end);

    histograms_[thread_idx].Add(duration * HISTOGRAM_TIME_SCALE);
    finished_roundtrips_[thread_idx]++;

    assert(status.ok());
  }

  void ThreadFunc(int thread_idx) {
    TermList terms;
    SearchRequest grpc_request;
    grpc_request.set_n_results(10);
    grpc_request.set_return_snippets(true);
    grpc_request.set_n_snippet_passages(3);
    SearchReply reply;

    while (shutdown_state_[thread_idx]->shutdown == false) {
      terms = query_pool_array_->Next(thread_idx);
      grpc_request.clear_terms();
      for (int i = 0; i < terms.size(); i++) {
        grpc_request.add_terms(terms[i]);
      }

      UnarySearch(thread_idx, grpc_request, reply);

      if (save_reply_) {
        reply_pools_[thread_idx].push_back(reply);
      }
    }
  }
};



std::unique_ptr<QQEngineSyncClient> CreateSyncClient(const std::string &target);
std::unique_ptr<AsyncClient> CreateAsyncClient(const GeneralConfig &config,
    std::unique_ptr<QueryPoolArray> query_pool_array);
std::unique_ptr<SyncUnaryClient> CreateSyncUnaryClient(const GeneralConfig &config,
    std::unique_ptr<QueryPoolArray> query_pool_array);
std::unique_ptr<Client> CreateClient(const GeneralConfig &config,
    std::unique_ptr<QueryPoolArray> query_pool_array);

#endif

