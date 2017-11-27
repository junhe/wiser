#ifndef QQ_ENGINE_H
#define QQ_ENGINE_H

#include <string>
#include <iostream>

#include "inverted_index.h"
#include "native_doc_store.h"


// This class is used by server side QQEngineServiceImpl.
// I could have put the functionality here in QQEngineServiceImpl, but
// I want to minimize entanglements with gRPC.
// Also, it is easier to test since it does not involve gRPC.
class QQSearchEngine {
    private:
        int next_doc_id_ = 0;

    public:
        NativeDocStore doc_store_;
        InvertedIndex inverted_index_;

        void AddDocument(const std::string &title, const std::string &url, 
                const std::string &body);
        std::string GetDocument(const int &doc_id);
        int NextDocId();
        std::vector<int> Search(const TermList &terms, const SearchOperator &op);
};

#endif
