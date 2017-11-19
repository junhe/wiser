#include "native_doc_store.h"

#include <string>
#include <iostream>

using namespace std;

void NativeDocStore::Add(int id, std::string document) {
    store_[id] = document;
}

void NativeDocStore::Remove(int id) {
    store_.erase(id);
}

bool NativeDocStore::Has(int id) {
    return store_.count(id) == 1;
}

std::string NativeDocStore::Get(int id) {
    return store_[id];
}

void NativeDocStore::Clear() {
    store_.clear();
}

int NativeDocStore::Size() {
    return store_.size();
}


