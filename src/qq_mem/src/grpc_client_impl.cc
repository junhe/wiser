#include "grpc_client_impl.h"

#include <iomanip>

#include "utils.h"

static std::mutex w_mutex;
static int write_count = 0;


std::unique_ptr<AsyncClient> CreateAsyncClient(const GeneralConfig &config,
    std::unique_ptr<QueryPoolArray> query_pool_array) 
{
  std::unique_ptr<AsyncClient> client(
      new AsyncClient(config, std::move(query_pool_array)));
  return client;
}

std::unique_ptr<SyncUnaryClient> CreateSyncUnaryClient(const GeneralConfig &config,
    std::unique_ptr<QueryPoolArray> query_pool_array) 
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


