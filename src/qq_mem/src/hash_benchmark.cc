#include "hash_benchmark.h"

#include "utils.h"


int main(int argc, char **argv) {
    STLHash stl_hash;

    std::size_t n = 5000000;
    int value_size = 32;

    build_hash(stl_hash, n, value_size);

    auto t1 = utils::now();
    query_hash(stl_hash, n, value_size);
    auto t2 = utils::now();

    auto duration = utils::duration(t1, t2);
    std::cout << "duration: " << duration << std::endl;
    std::cout << "Speed: " << n / duration << std::endl;
    std::cout << "Finished" << std::endl;
    return 0;
}


