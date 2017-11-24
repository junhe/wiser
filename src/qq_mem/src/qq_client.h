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

// This is a wrapper around the gRPC stub to provide more convenient
// Interface to interact with gRPC server. For example, you can use
// C++ native containers intead of protobuf objects when invoking
// AddDocument().


class QQEngineClient {
    public:
        QQEngineClient(std::shared_ptr<Channel> channel)
            : stub_(QQEngine::NewStub(channel)) {}

        std::string AddDocument();
        std::string Search();
    private:
        std::unique_ptr<QQEngine::Stub> stub_;
};

#endif
