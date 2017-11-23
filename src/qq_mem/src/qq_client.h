#ifndef QQ_CLIENT_H
#define QQ_CLIENT_H

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "qq.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using qq::QQEngine;

// for AddDocument
using qq::AddDocumentRequest;
using qq::StatusReply;

// for Search
using qq::SearchRequest;
using qq::SearchReply;


class QQEngineClient {
    public:
        QQEngineClient(std::shared_ptr<Channel> channel)
            : stub_(QQEngine::NewStub(channel)) {}

        std::string AddDocument() {
            AddDocumentRequest request;
            request.mutable_document()->set_title("My first document");
            request.mutable_document()->set_url("http://wikipedia.org");
            request.mutable_document()->set_body("BIG body");

            request.mutable_options()->set_save(true);

            StatusReply reply;

            ClientContext context;

            // Here we can the stub's newly available method we just added.
            Status status = stub_->AddDocument(&context, request,  &reply);
            if (status.ok()) {
                return reply.message();
            } else {
                std::cout << status.error_code() << ": " << status.error_message()
                    << std::endl;
                return "RPC failed";
            }
        }

        std::string Search() {
            SearchRequest request;
            SearchReply reply;
            ClientContext context;

            request.set_term("body");

            // Here we can the stub's newly available method we just added.
            Status status = stub_->Search(&context, request,  &reply);
            if (status.ok()) {
                std::cout << "Search result: ";
                for (auto id : reply.doc_ids()) {
                    std::cout << id << " ";
                }
                std::cout << std::endl;
                std::cout << "----------" << std::endl;
                return "OK";
            } else {
                std::cout << status.error_code() << ": " << status.error_message()
                    << std::endl;
                return "RPC failed";
            }
        }

    private:
        std::unique_ptr<QQEngine::Stub> stub_;
};




#endif
