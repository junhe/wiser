#include "grpc_client_impl.h"

#include <iomanip>

#include "utils.h"

static std::mutex w_mutex;
static int write_count = 0;


std::unique_ptr<AsyncClient> CreateAsyncClient(const GeneralConfig &config,
    std::unique_ptr<TermPoolArray> query_pool_array) 
{
  std::unique_ptr<AsyncClient> client(
      new AsyncClient(config, std::move(query_pool_array)));
  return client;
}


std::unique_ptr<SyncUnaryClient> CreateSyncUnaryClient(const GeneralConfig &config,
    std::unique_ptr<TermPoolArray> query_pool_array) 
{
  std::unique_ptr<SyncUnaryClient> client(
      new SyncUnaryClient(config, std::move(query_pool_array)));
  return client;
}


std::unique_ptr<QQEngineSyncClient> CreateSyncClient(const std::string &target) {
  std::unique_ptr<QQEngineSyncClient> client(
      new QQEngineSyncClient(
        grpc::CreateChannel(target, grpc::InsecureChannelCredentials())));
  return client;
}


std::unique_ptr<QQEngine::Stub> CreateStub(const std::string &target) {
  return QQEngine::NewStub(
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
}


std::unique_ptr<Client> CreateClient(const GeneralConfig &config,
    std::unique_ptr<TermPoolArray> query_pool_array) {
  auto sync_type = config.GetString("synchronization");
  if (sync_type == "SYNC") {
    auto arity = config.GetString("rpc_arity");
    if (arity == "UNARY") {
      std::unique_ptr<Client> client(
          new SyncUnaryClient(config, std::move(query_pool_array)));
      return client;
    } else if (arity == "STREAMING") {
      std::unique_ptr<Client> client(
          new SyncStreamingClient(config, std::move(query_pool_array)));
      return client;
    }
  } else if (config.GetString("synchronization") == "ASYNC") {
      std::unique_ptr<Client> client(
          new AsyncClient(config, std::move(query_pool_array)));
      return client;
  }
}


