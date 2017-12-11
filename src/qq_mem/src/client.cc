#include <chrono>
#include <thread>

#include "qq_client.h"
#include "index_creator.h"


void make_queries(int n_queries) {
    std::string reply;

    QQEngineSyncClient qqengine(grpc::CreateChannel(
                "localhost:50051", grpc::InsecureChannelCredentials()));


    for (int i = 0; i < n_queries; i++) {
        reply = qqengine.Search("hello");
    }

}



int main(int argc, char** argv) {
    // QQEngineSyncClient qqengine(grpc::CreateChannel(
                // "localhost:50051", grpc::InsecureChannelCredentials()));
    auto qqengine = CreateSyncClient("localhost:50051");

    // IndexCreator index_creator("src/testdata/tokenized_wiki_abstract_line_doc", qqengine);
    // IndexCreator index_creator("/mnt/ssd/downloads/test_doc_tokenized", qqengine);
    IndexCreator index_creator("/mnt/ssd/downloads/enwiki-abstract_tokenized.linedoc", *qqengine);
    index_creator.DoIndex();

    std::vector<std::thread> threadList;


    auto start = std::chrono::system_clock::now();
    int n_threads = 16;
    int n_queries = 50000;
    for(int i = 0; i < n_threads; i++)
    {
        threadList.push_back( std::thread(make_queries, n_queries) );
    }
    std::for_each(threadList.begin(), threadList.end(), std::mem_fn(&std::thread::join));

    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> diff = end - start;

    std::cout << "Duration: " << diff.count() << " s\n";
    std::cout << "Query per sec: " << n_queries * n_threads / diff.count() << "\n";

    return 0;
}

