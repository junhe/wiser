#include "hash_benchmark.h"


int main(int argc, char **argv) {

    STLHash stl_hash;

    build_hash(stl_hash, 2);
    query_hash(stl_hash, 2);

    return 0;
}


