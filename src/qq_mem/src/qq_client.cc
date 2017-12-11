#include "qq_client.h"


bool QQEngineSyncClient::AddDocument(const std::string &title, 
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


bool QQEngineSyncClient::Search(const std::string &term) {
    SearchRequest request;
    SearchReply reply;
    ClientContext context;

    request.set_term(term);

    // Here we can the stub's newly available method we just added.
    Status status = stub_->Search(&context, request,  &reply);
    return status.ok();
}


bool QQEngineSyncClient::Echo(const EchoData &request, EchoData &reply) {
    ClientContext context;

    Status status = stub_->Echo(&context, request,  &reply);
    if (status.ok()) {
        return true;
    } else {
        std::cout << status.error_code() 
            << ": " << status.error_message()
            << std::endl;
        return false;
    }
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



