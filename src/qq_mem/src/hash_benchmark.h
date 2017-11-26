#ifndef HASH_BENCHMARK_H
#define HASH_BENCHMARK_H

#include <iostream>
#include <string>
#include <unordered_map>

class HashService {
  public:
    virtual std::string Get(const std::string &key) = 0;
    virtual void Put(const std::string &key, const std::string &value ) = 0;
    virtual std::size_t Size() = 0;
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
};

std::string encode(std::size_t i) {
    return std::to_string(i);
}

void build_hash(HashService &hash, std::size_t n) {
    std::string value = "0123456789";
    for (std::size_t i; i < n; i++) {
        hash.Put(encode(i), value);
    }
}

void query_hash(HashService &hash, std::size_t n) {
    std::string value;
    for (std::size_t i; i < n; i++) {
        value = hash.Get(encode(i));
    }
}


#endif
