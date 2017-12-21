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
        int offsets_flag_ = 0;        // whether store offsets in postings

        QQSearchEngine(){Global_Posting_Store = new FlashReader(POSTINGS_ON_FLASH_FILE); Global_Document_Store = new FlashReader(DOCUMENTS_ON_FLASH_FILE);}    // TODO init the posting store here
        void AddDocument(const std::string &title, const std::string &url, 
                const std::string &body);
        void AddDocument(const std::string &title, const std::string &url, 
                const std::string &body, const std::string &tokens, const std::string &offsets);
        const std::string & GetDocument(const int &doc_id);
        const Passage_Segments & GetDocumentPassages(const int &doc_id);    // get precompute passages of document
        int NextDocId();
        std::vector<int> Search(const TermList &terms, const SearchOperator &op);


    private:
        void precompute_insert_splits(const Passage_Segments & splits, Offsets & offsets_in);
        
};

#endif
