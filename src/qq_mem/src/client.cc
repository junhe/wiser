#include <chrono>

#include "qq_client.h"
#include "index_creator.h"



void make_queries(QQEngineClient &qqengine) {
    std::string reply;
    int n_queries = 10000;

    auto start = std::chrono::system_clock::now();

    for (int i = 0; i < n_queries; i++) {
        reply = qqengine.Search("hello");
    }

    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> diff = end - start;
    std::cout << "Duration: " << diff.count() << " s\n";
    std::cout << "Query per sec: " << n_queries / diff.count() << "\n";

    std::cout << "Search: " << reply << std::endl;
}



int main(int argc, char** argv) {
    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).
    QQEngineClient qqengine(grpc::CreateChannel(
                "localhost:50051", grpc::InsecureChannelCredentials()));


    // IndexCreator index_creator("src/testdata/tokenized_wiki_abstract_line_doc", qqengine);
    IndexCreator index_creator("/mnt/ssd/downloads/test_doc_tokenized", qqengine);
    index_creator.DoIndex();

    make_queries(qqengine);

    return 0;
}
