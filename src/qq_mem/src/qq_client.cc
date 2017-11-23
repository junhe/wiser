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
