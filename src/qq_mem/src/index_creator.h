#ifndef INDEX_CREATOR_H
#define INDEX_CREATOR_H

#include <string>

#include "qq_client.h"

// This class reads original documents and make RPC calls to index them
class IndexCreator {
    private:
        std::string line_doc_path_;
        QQEngineSyncClient &client_;

    public:
        IndexCreator(const std::string &line_doc_path, QQEngineSyncClient &client);
        void DoIndex();
};

#endif
