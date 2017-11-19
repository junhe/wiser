#include "inverted_index.h"

void InvertedIndex::AddDocument(const int &doc_id, const TermList &termlist) {
    for (const auto &term : termlist) {
        auto search = index_.find(term);
        IndexStore::iterator it;

        if (search == index_.cend()) {
            // term does not exist
            std::pair<IndexStore::iterator, bool> ret;
            ret = index_.insert( std::make_pair(term, PostingList(term)) );
            it = ret.first;
        } else {
            it = index_.find(term);
        }
        
        it->second.AddPosting(doc_id, 0, Positions{});
    }
}

void InvertedIndex::GetPostingList(const int &doc_id) {
}

void InvertedIndex::Search(const TermList &terms, const SearchOperator &op) {
}

