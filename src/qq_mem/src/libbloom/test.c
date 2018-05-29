#include <stdio.h>
#include "bloom.h"

int main() {
    
    struct bloom bloom;


    bloom_init(&bloom, 100, 0.1);
    bloom_add(&bloom, "hello", 6);

    printf("overall bytes: %d %d \n", bloom.bytes, bloom.hashes);
    printf("overall bytes: %s \n", bloom.bf);
    if (bloom_check(&bloom, "pelle", 6)) {
      printf("It may be there!\n");
    }
    
    struct bloom bloom_test;
    //bloom_init(&bloom_test, 100, 0.1);
    //bloom_test.bf = bloom.bf;
    bloom_set(&bloom_test, 100, 0.1, bloom.bf);
    if (bloom_check(&bloom_test, "pelle", 6)) {
      printf("It may be there!\n");
    }
    return 0;
} 
