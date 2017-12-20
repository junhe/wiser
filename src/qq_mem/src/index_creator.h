#ifndef INDEX_CREATOR_H
#define INDEX_CREATOR_H

#include <string>

#include "grpc_client_impl.h"

// This class reads original documents and make RPC calls to index them
class IndexCreator {
    private:
        std::string line_doc_path_;
        QQEngineSyncClient &client_;

    public:
        IndexCreator(const std::string &line_doc_path, QQEngineSyncClient &client);
        void DoIndex(int n_rows = -1);
};

#endif
