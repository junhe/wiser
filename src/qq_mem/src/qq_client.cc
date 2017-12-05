#include "qq_client.h"

std::string QQEngineClient::AddDocument(const std::string &title, 
        const std::string &url, const std::string &body, const std::string &offsets) {
    AddDocumentRequest request;
    request.mutable_document()->set_title(title);
    request.mutable_document()->set_url(url);
    request.mutable_document()->set_body(body);
    request.mutable_document()->set_offsets(offsets);

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


std::string QQEngineClient::Search(const std::string &term) {
    SearchRequest request;
    SearchReply reply;
    ClientContext context;

    request.set_term(term);

    // Here we can the stub's newly available method we just added.
    Status status = stub_->Search(&context, request,  &reply);
    if (status.ok()) {
        // std::cout << "Search result: ";
        // for (auto id : reply.doc_ids()) {
            // std::cout << id << " ";
        // }
        return "OK";
    } else {
        std::cout << status.error_code() << ": " << status.error_message()
            << std::endl;
        return "RPC failed";
    }
}
