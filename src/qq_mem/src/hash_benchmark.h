#ifndef HASH_BENCHMARK_H
#define HASH_BENCHMARK_H

#include <iostream>
#include <string>
#include <unordered_map>
#include <cassert>

#include "utils.h"

class HashService {
  public:
    virtual std::string Get(const std::string &key) = 0;
    virtual void Put(const std::string &key, const std::string &value ) = 0;
    virtual std::size_t Size() = 0;
    virtual void Find(const std::string &key) = 0;
};

class STLHash: public HashService {
  public:
    typedef std::unordered_map<std::string, std::string> HashType;
    HashType hash_;

    std::string Get(const std::string &key) {
        return hash_.at(key);
    }

    void Put(const std::string &key, const std::string &value ) {
        // You must make sure `key` does not exist, otherwise, 
        // value will not be updated.
        hash_.insert({key, value});
            // std::make_pair<std::string, std::string>(key, value));
    }

    std::size_t Size() {
        return hash_.size();
    }

    void Find(const std::string &key) {
      volatile auto it = hash_.find(key);
    }
};

std::string encode(std::size_t i, int width) {
    return utils::fill_zeros(std::to_string(i), width);
}

std::string compose_value(int value_size) {
    return std::string(value_size, 'v');
}

void build_hash(HashService &hash, std::size_t n, int key_size, int value_size) {
    std::string value = compose_value(value_size);
    std::cout << "build hash " << n << "\n";
    for (std::size_t i = 0; i < n; i++) {
        hash.Put(encode(i, key_size), value);
    }
}

void query_hash(HashService &hash, std::size_t n, int key_size, int value_size) {
    std::string value;
    std::string good_value = compose_value(value_size);
    std::string k = encode(88, key_size);
    for (std::size_t i = 0; i < n; i++) {
        // value = hash.Get(encode(i, key_size));
        hash.Find(k);

        // if (i % 100000 == 0) {
            // std::cout << i << "/" << n << std::endl;
        // }
        // assert(value == good_value);
    }
}


#endif
