#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "qq.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using qq::HelloRequest;
using qq::HelloReply;
using qq::QQEngine;

// for AddDocument
using qq::AddDocumentRequest;
using qq::StatusReply;

class QQEngineClient {
    public:
        QQEngineClient(std::shared_ptr<Channel> channel)
            : stub_(QQEngine::NewStub(channel)) {}

        // Assembles the client's payload, sends it and presents the response back
        // from the server.
        std::string SayHello(const std::string& user) {
            // Data we are sending to the server.
            HelloRequest request;
            request.set_name(user);

            // Container for the data we expect from the server.
            HelloReply reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // The actual RPC.
            Status status = stub_->SayHello(&context, request, &reply);

            // Act upon its status.
            if (status.ok()) {
                return reply.message();
            } else {
                std::cout << status.error_code() << ": " << status.error_message()
                    << std::endl;
                return "RPC failed";
            }
        }

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


    private:
        std::unique_ptr<QQEngine::Stub> stub_;
};

int main(int argc, char** argv) {
    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).
    QQEngineClient qqengine(grpc::CreateChannel(
                "localhost:50051", grpc::InsecureChannelCredentials()));
    std::string user("world");
    std::string reply = qqengine.SayHello(user);
    std::cout << "QQEngine received: " << reply << std::endl;

    reply = qqengine.AddDocument();
    std::cout << "Greeter received: " << reply << std::endl;

    return 0;
}