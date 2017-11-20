#ifndef QQ_ENGINE_HH
#define QQ_ENGINE_HH

#include <string>
#include <iostream>

#include "inverted_index.h"
#include "native_doc_store.h"


// This class is used by server side QQEngineServiceImpl.
// I could have put the functionality here in QQEngineServiceImpl, but
// I want to minimize entanglements with gRPC.
class QQEngine {
    public:
        NativeDocStore doc_store_;
        InvertedIndex inverted_index_;

        void AddDocument(const std::string &title, const std::string &url, 
                const std::string &body);
};

#endif
