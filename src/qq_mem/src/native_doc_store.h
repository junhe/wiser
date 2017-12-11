#ifndef NATIVE_DOC_STORE_HH
#define NATIVE_DOC_STORE_HH

#include <map>

#include "engine_services.h"

class NativeDocStore: public DocumentStoreService {
    private:
        std::map<int,std::string> store_;
        std::map<int,Passage_Segements> passages_store_;

    public:
        NativeDocStore() {}

        void Add(int id, std::string document);
        void Remove(int id);
        std::string & Get(int id);
        Passage_Segements & GetPassages(int id);
        bool Has(int id);
        void Clear();
        int Size();
};

#endif
