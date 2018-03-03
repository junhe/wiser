#include <chrono>
#include <thread>
#include <cassert>

#include "grpc_client_impl.h"
#include "utils.h"

//////////////////////////////////////
// This benchmark does sync echos.
//////////////////////////////////////

void make_queries(int n_queries) {
    EchoData request, reply;
    request.set_message("");

    QqGrpcPlainClient qqengine(grpc::CreateChannel(
                "localhost:50051", grpc::InsecureChannelCredentials()));

    for (int i = 0; i < n_queries; i++) {
        // reply = qqengine.Search("hello");
        qqengine.Echo(request, reply);
        // std::cout << reply << std::endl;
        // assert(reply == "OK");
    }
}

void benchmark(int n_threads, int n_queries) {
    std::vector<std::thread> threadList;

    auto start = utils::now();

    for(int i = 0; i < n_threads; i++)
    {
        threadList.push_back( std::thread(make_queries, n_queries) );
    }
    std::for_each(threadList.begin(), threadList.end(), std::mem_fn(&std::thread::join));

    auto end = utils::now();

    auto duration = utils::duration(start, end);

    std::cout << "_____________________________" << std::endl;
    std::cout << "n_threads: " << n_threads << " n_queries: " << n_queries << std::endl;
    std::cout << "Duration: " << duration << " s\n";
    std::cout << "Query per sec: " << n_queries * n_threads / duration << "\n";
}


int main(int argc, char** argv) {
    int total = 100000;
    benchmark(1, total);
    benchmark(4, total);
    benchmark(16, total);
    benchmark(64, total);
    benchmark(128, total);
}
