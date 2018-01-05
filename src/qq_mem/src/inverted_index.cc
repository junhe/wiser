#include "inverted_index.h"
#include "posting_list_protobuf.h"

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

    std::cout << "==== Get in search" << std::endl;
    std::vector<int> doc_ids;
    for (auto &term : terms) {
        auto tmp_doc_ids = GetDocumentIds(term);
        doc_ids.insert(doc_ids.end(), tmp_doc_ids.begin(), tmp_doc_ids.end()); 
    }

    std::set<int> id_set(doc_ids.begin(), doc_ids.end());
    std::cout << "size of results: " << *id_set.begin() << std::endl;
    return std::vector<int> (id_set.begin(), id_set.end());
}

// Get the posting according to term and docID
const Posting & InvertedIndex::GetPosting(const Term & term, const int & doc_id)  {
    // TODO check whether has this term
    if (FLAG_POSTINGS_ON_FLASH) {
        // Check whether this Posting is in cache
        std::string this_key = construct_key(term, doc_id);
        if (_postings_cache_.exists(this_key)) {
            return _postings_cache_.get(this_key);
        }
        // Not in cache, read from flash
        // get postion
        const Store_Segment stored_position = (index_.find(term)->second).GetPostingStorePosition(doc_id);
        //std::cout << "--IO-- load Posting From flash" << std::endl;
        // read, parse -> add to cache
        if (POSTING_SERIALIZATION == "protobuf")
            _postings_cache_.put(this_key, parse_protobuf_string_to_posting(Global_Posting_Store->read(stored_position)));
        if (POSTING_SERIALIZATION == "cereal")
            _postings_cache_.put(this_key, parse_cereal_string_to_posting(Global_Posting_Store->read(stored_position)));
           
        // return posting        TODO Lock Problems(should not kick out a posting from cache while still using it)
        return _postings_cache_.get(this_key);
    } else {
        return (index_.find(term)->second).GetPosting(doc_id);
    }
}

const Posting InvertedIndex::parse_cereal_string_to_posting(const std::string & serialized) {
    std::stringstream ss; // any stream can be used
    ss.str(serialized);

    cereal::BinaryInputArchive iarchive(ss); // Create an input archive

    Posting new_posting;
    iarchive(new_posting); // Read the data from the archive
    
    return new_posting;
}

const Posting InvertedIndex::parse_protobuf_string_to_posting(const std::string & serialized) {
    // parse string to protobuf message
    posting_message::Posting_Precomputed_4_Snippets p_message;
    p_message.ParseFromString(serialized);
    
    // copy the message to data strucutre Posting
    Posting new_posting;
    new_posting.docID_ = p_message.docid();
    new_posting.term_frequency_ = p_message.term_frequency();

      // offsets
    for (int i = 0; i < p_message.offsets_size(); i++) {
        new_posting.positions_.push_back(Offset(p_message.offsets(i).start_offset(), p_message.offsets(i).end_offset()));
    }
      // passage scores
    for (int i = 0; i < p_message.passage_scores_size(); i++) {
        new_posting.passage_scores_.push_back(Passage_Score(p_message.passage_scores(i).passage_id(), p_message.passage_scores(i).score()));
    } 

      // passage splits
    for (auto split = p_message.passage_splits().begin(); split != p_message.passage_splits().end(); split++) {
        int key = split->first;
        new_posting.passage_splits_.insert({key, Passage_Split(split->second.start_offset(), split->second.len())});
    }
    return new_posting;
}

std::string InvertedIndex::construct_key(const Term & term, const int & docID) {
    std::string res = term + "_" + std::to_string(docID);
    return res;
}

IndexStore::const_iterator InvertedIndex::Find(const Term &term) {
    return index_.find(term);
}

IndexStore::const_iterator InvertedIndex::ConstEnd() {
    return index_.cend();
}
