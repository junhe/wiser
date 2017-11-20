#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "qq.grpc.pb.h"
#include "qq_engine.h"
#include "inverted_index.h"
#include "native_doc_store.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using qq::HelloRequest;
using qq::HelloReply;

// for AddDocument
using qq::AddDocumentRequest;
using qq::StatusReply;

// for Search
using qq::SearchRequest;
using qq::SearchReply;

using qq::QQEngine;



// Logic and data behind the server's behavior.
class QQEngineServiceImpl final : public QQEngine::Service {
    Status SayHello(ServerContext* context, const HelloRequest* request,
            HelloReply* reply) override {
        std::string prefix("Hello ");
        reply->set_message(prefix + request->name());
        return Status::OK;
    }

    Status AddDocument(ServerContext* context, const AddDocumentRequest* request,
            StatusReply* reply) override {
        std::cout << "title" << request->document().title() << std::endl;
        std::cout << "url" << request->document().url() << std::endl;
        std::cout << "body" << request->document().body() << std::endl;

        search_engine_.AddDocument(request->document().title(),
                request->document().url(), request->document().body());

        reply->set_message("Doc added");
        reply->set_ok(true);
        return Status::OK;
    }


    Status Search(ServerContext* context, const SearchRequest* request,
            SearchReply* reply) override {
        Term term = request->term();

        std::cout << "search term: " << term << std::endl;

        auto doc_ids = search_engine_.Search(TermList{term}, SearchOperator::AND);
        
        for (auto id : doc_ids) {
            reply->add_doc_ids(id);

            std::string doc = search_engine_.GetDocument(id);
            std::cout << "Document found: " << doc << std::endl;
        }
        return Status::OK;
    }


    public:
        QQSearchEngine search_engine_;
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    QQEngineServiceImpl service;

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    RunServer();

    return 0;
}
