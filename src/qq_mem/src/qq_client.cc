#include "qq_client.h"


std::string QQEngineClient::AddDocument() {
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


std::string QQEngineClient::Search() {
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



int main(int argc, char** argv) {
    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).
    QQEngineClient qqengine(grpc::CreateChannel(
                "localhost:50051", grpc::InsecureChannelCredentials()));

    std::string reply = qqengine.AddDocument();
    std::cout << "Greeter received: " << reply << std::endl;

    reply = qqengine.Search();
    std::cout << "Search: " << reply << std::endl;

    return 0;
}
