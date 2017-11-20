#include "inverted_index.h"

#include <set>

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

        PostingList &postinglist = it->second;        
        postinglist.AddPosting(doc_id, 0, Positions{});
    }
}

std::vector<int> InvertedIndex::GetDocumentIds(const Term &term) {
    auto it = index_.find(term);

    if (it == index_.cend()) {
        // term does not exist
        return std::vector<int>{};
    }

    // Have the term
    PostingList &pl = it->second;
    std::vector<int> doc_ids;

    for (const auto posting_pair : pl.GetPostings()) {
        doc_ids.push_back(posting_pair.second.GetDocId());
    }
  
    return doc_ids;
}

// Return the document IDs
std::vector<int> InvertedIndex::Search(const TermList &terms, const SearchOperator &op) {
    std::vector<int> doc_ids;
    for (auto &term : terms) {
        auto tmp_doc_ids = GetDocumentIds(term);
        doc_ids.insert(doc_ids.end(), tmp_doc_ids.begin(), tmp_doc_ids.end()); 
    }

    std::set<int> id_set(doc_ids.begin(), doc_ids.end());
    return std::vector<int> (id_set.begin(), id_set.end());
}

