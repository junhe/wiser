#ifndef QQ_MEM_UNCOMPRESSED_ENGINE_H
#define QQ_MEM_UNCOMPRESSED_ENGINE_H

#include "intersect.h"
#include "native_doc_store.h"

// It returns the number of terms in field
int count_terms(const std::string field) {
  return utils::explode(field, ' ').size();
}


typedef std::unordered_map<Term, int> CountMapType;
CountMapType count_tokens(const std::string &token_text) {
  CountMapType counts;
  auto tokens = utils::explode(token_text, ' ');

  for (auto token : tokens) {
    auto it = counts.find(token);
    if (it == counts.end()) {
      counts[token] = 1;
    } else {
      it->second++;
    }
  }
  return counts;
}



class InvertedIndexQqMem {
 private:
  typedef PostingList_Vec<RankingPosting> PostingListType;
  typedef std::unordered_map<Term, PostingListType> IndexStore;
  IndexStore index_;

 public:
  typedef IndexStore::const_iterator const_iterator;

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens) {
    TermList token_vec = utils::explode(tokens, ' ');
    CountMapType token_counts = count_tokens(tokens);

    for (auto token_it = token_counts.begin(); token_it != token_counts.end(); 
        token_it++) {
      IndexStore::iterator it;
      auto term = token_it->first;
      it = index_.find(term);

      if (it == index_.cend()) {
        // term does not exist
        std::pair<IndexStore::iterator, bool> ret;
        ret = index_.insert( std::make_pair(term, PostingListType(term)) );
        it = ret.first;
      } 

      PostingListType &postinglist = it->second;        
      postinglist.AddPosting(RankingPosting(doc_id, token_it->second));
    }
    int total_terms = count_terms(tokens);
    std::cout << "field length(total terms): " << total_terms << std::endl;
  }

  std::vector<DocIdType> Search(const TermList &terms, const SearchOperator &op) {
    if (op != SearchOperator::AND) {
        throw std::runtime_error("NotImplementedError");
    }

    std::vector<const PostingList_Vec<RankingPosting>*> postinglist_pointers;

    for (auto term : terms) {
      auto it = index_.find(term);
      if (it == index_.end()) {
        std::cout << "No match at all" << std::endl;
        return std::vector<DocIdType>{};
      }

      postinglist_pointers.push_back(&it->second);
    }

    TfIdfStore tfidf_store;

    return intersect<RankingPosting>(postinglist_pointers, &tfidf_store);
  }

  void ShowStats() {
    LOG(INFO) << "num of terms: " << index_.size() << std::endl;
    for (auto it = index_.begin(); it != index_.end(); it++) {
      // LOG(INFO) << it->second.Size();
    }
  }
};


// Also, it is easier to test since it does not involve gRPC.
class QqMemUncompressedEngine {
 private:
  int next_doc_id_ = 0;
  NativeDocStore doc_store_;
  InvertedIndexQqMem inverted_index_;
  DocLengthStore doc_lengths_;

  int NextDocId() {
    return next_doc_id_++;
  }

 public:
  void AddDocument(const std::string &body, const std::string &tokens) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, body);
    inverted_index_.AddDocument(doc_id, body, tokens);
    doc_lengths_.AddLength(doc_id, count_terms(body));
  }
};



#endif

