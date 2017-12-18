#include "inverted_index.h"

#include <set>
#include <iostream>

void InvertedIndex::AddDocument(const int &doc_id, const TermWithOffsetList &termlist) {
    for (const auto &term_with_offset : termlist) {
        Term term = term_with_offset.term_;
        auto search = index_.find(term);
        IndexStore::iterator it;

        if (search == index_.cend()) {
            // term does not exist
            std::pair<IndexStore::iterator, bool> ret;
            ret = index_.insert( std::make_pair(term, PostingList_Direct(term)) );
            it = ret.first;
        } else {
            it = index_.find(term);  // TODO why find again? this can cost time
        }

        PostingList_Direct &postinglist = it->second;
        postinglist.AddPosting(doc_id, term_with_offset.offsets_.size(), term_with_offset.offsets_);
    }
}

void InvertedIndex::AddDocument(const int &doc_id, const TermList &termlist) {
    for (const auto &term : termlist) {
        auto search = index_.find(term);
        IndexStore::iterator it;

        if (search == index_.cend()) {
            // term does not exist
            std::pair<IndexStore::iterator, bool> ret;
            ret = index_.insert( std::make_pair(term, PostingList_Direct(term)) );
            it = ret.first;
        } else {
            it = index_.find(term);  // TODO why find again? this can cost time
        }

        PostingList_Direct &postinglist = it->second;        
        postinglist.AddPosting(doc_id, 0, Offsets{});
    }
}

std::vector<int> InvertedIndex::GetDocumentIds(const Term &term) {
    auto it = index_.find(term);

    if (it == index_.cend()) {
        // term does not exist
        return std::vector<int>{};
    }

    // Have the term
    PostingList_Direct &pl = it->second;
    std::vector<int> doc_ids;

    for (auto it = pl.begin(); it != pl.end(); it++) {
        auto posting = pl.ExtractPosting(it);
        doc_ids.push_back(posting.docID_);
    }

    return doc_ids;
}

// Return the document IDs
std::vector<int> InvertedIndex::Search(const TermList &terms, const SearchOperator &op) {
    if (op != SearchOperator::AND) {
        // TODO: define an exception class
        throw "NotImplementedError";
    }

    std::vector<int> doc_ids;
    for (auto &term : terms) {
        auto tmp_doc_ids = GetDocumentIds(term);
        doc_ids.insert(doc_ids.end(), tmp_doc_ids.begin(), tmp_doc_ids.end()); 
    }

    std::set<int> id_set(doc_ids.begin(), doc_ids.end());
    return std::vector<int> (id_set.begin(), id_set.end());
}

// Get the posting according to term and docID
const Posting & InvertedIndex::GetPosting(const Term & term, const int & doc_id)  {
    // TODO check whether has this term
    if (FLAG_POSTINGS_ON_FLASH) {
        // Check whether this Posting is in cache
        std::string this_key = construct_key({term}, doc_id);
        std::cout << "key: " << this_key << std::endl;
        if (_postings_cache_.exists(this_key)) {
           return _postings_cache_.get(this_key);
        }
        // Not in cache, read from flash
        // get postion
        // read, parse 
        // add to cache
        // return posting        TODO Lock Problems(should not kick out a posting from cache while still using it)

    } else {
        return (index_.find(term)->second).GetPosting(doc_id);
    }
}

std::string InvertedIndex::construct_key(const Term & term, const int & docID) {
    std::string res = term + "_" + std::to_string(docID);
    return res;
}
