#ifndef NATIVE_DOC_STORE_HH
#define NATIVE_DOC_STORE_HH

#include <string>

#include "engine_services.hh"

class NativeDocStore: public DocumentStoreService {
    public:
        NativeDocStore() {}

        void AddDocument(int id, std::string document);
        void RemoveDocument(int id);
        bool HasDocument(int id);
        void Clear();
};

#endif
