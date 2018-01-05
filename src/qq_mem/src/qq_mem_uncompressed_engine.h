#ifndef QQ_MEM_UNCOMPRESSED_ENGINE_H
#define QQ_MEM_UNCOMPRESSED_ENGINE_H

#include "intersect.h"
#include "utils.h"
#include "native_doc_store.h"

class InvertedIndexQqMem {
 private:
  typedef PostingList_Vec<RankingPosting> PostingListType;
  typedef std::unordered_map<Term, PostingListType> IndexStore;
  typedef std::vector<const PostingList_Vec<RankingPosting>*> PlPointers;
  IndexStore index_;

  PlPointers FindPostinglists(const TermList &terms) {
    PlPointers postinglist_pointers;

    for (auto term : terms) {
      auto it = index_.find(term);
      if (it == index_.end()) {
        break;
      }

      postinglist_pointers.push_back(&it->second);
    }

    return postinglist_pointers;
  }

  TfIdfStore IntersectPostinglists(const PlPointers &postinglist_pointers) {
    TfIdfStore tfidf_store;
    intersect<RankingPosting>(postinglist_pointers, &tfidf_store);
    return tfidf_store;
  }

 public:
  typedef IndexStore::const_iterator const_iterator;

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens) {
    TermList token_vec = utils::explode(tokens, ' ');
    utils::CountMapType token_counts = utils::count_tokens(tokens);

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
  }

  // This function returns True if we find documents in the intersection.
  // If it returns True, tfidf_store will be set.
  //
  // terms must have unique terms.
  TfIdfStore FindIntersection(const TermList &terms) {
    PlPointers postinglist_pointers = FindPostinglists(terms);
    TfIdfStore tfidf_store;
    if (postinglist_pointers.size() < terms.size()) {
      return tfidf_store; // return an empty one
    }

    intersect<RankingPosting>(postinglist_pointers, &tfidf_store);
    return tfidf_store;
  }

  std::vector<DocIdType> Search(const TermList &terms, const SearchOperator &op) {
    if (op != SearchOperator::AND) {
        throw std::runtime_error("NotImplementedError");
    }

    std::vector<DocIdType> doc_ids;
    PlPointers postinglist_pointers = FindPostinglists(terms);

    return intersect<RankingPosting>(postinglist_pointers, nullptr);
  }

  // Return number of posting lists
  std::size_t Size() const {
    return index_.size();
  }
};


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
  int LoadLocalDocuments(const std::string &line_doc_path, int n_rows) {
    utils::LineDoc linedoc(line_doc_path);
    std::vector<std::string> items;
    bool has_it;

    int count = 0;
    for (int i = 0; i < n_rows; i++) {
      has_it = linedoc.GetRow(items);
      if (has_it) {
        AddDocument(items[2], items[2]);
        count++;
      } else {
        break;
      }

      if (count % 10000 == 0) {
        std::cout << "Indexed " << count << " documents" << std::endl;
      }
    }

    return count;
  }

  DocIdType AddDocument(const std::string &body, const std::string &tokens) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, body);
    inverted_index_.AddDocument(doc_id, body, tokens);
    doc_lengths_.AddLength(doc_id, utils::count_terms(body));
    return doc_id;
  }

  std::string GetDocument(const DocIdType &doc_id) {
    return doc_store_.Get(doc_id);
  }

  std::size_t TermCount() {
    return inverted_index_.Size();
  }

  int GetDocLength(const DocIdType &doc_id) {
    return doc_lengths_.GetLength(doc_id);
  }

  TfIdfStore Query(const TermList &terms) {
    return inverted_index_.FindIntersection(terms);
  }

  DocScoreVec Score(const TfIdfStore &tfidf_store) {
    return score_docs(tfidf_store, doc_lengths_);
  }

  std::vector<DocIdType> FindTopK(const DocScoreVec &doc_scores, int k) {
    return utils::find_top_k(doc_scores, k);
  }

  std::vector<DocIdType> SearchWithoutSnippet(const TermList &terms) {
    auto tfidf_store = Query(terms);
    auto scores = Score(tfidf_store);
    return FindTopK(scores, 10);
  }
};


#endif

