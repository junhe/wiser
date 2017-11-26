#include <chrono>
#include <thread>
#include <cassert>

#include "qq_client.h"
#include "index_creator.h"
#include "utils.h"


void make_queries(int n_queries) {
    std::string reply;

    QQEngineClient qqengine(grpc::CreateChannel(
                "localhost:50051", grpc::InsecureChannelCredentials()));

    for (int i = 0; i < n_queries; i++) {
        // reply = qqengine.Search("hello");
        reply = qqengine.Echo("");
        // std::cout << reply << std::endl;
        // assert(reply == "OK");
    }
}


int main(int argc, char** argv) {
    std::vector<std::thread> threadList;
    int n_threads = 32;
    int n_queries = 50000;

    auto start = utils::now();

    for(int i = 0; i < n_threads; i++)
    {
        threadList.push_back( std::thread(make_queries, n_queries) );
    }
    std::for_each(threadList.begin(), threadList.end(), std::mem_fn(&std::thread::join));

    auto end = utils::now();

    auto duration = utils::duration(start, end);

    std::cout << "Duration: " << duration << " s\n";
    std::cout << "Query per sec: " << n_queries * n_threads / duration << "\n";

    return 0;
}
