#ifndef POSTING_LIST_VEC_H
#define POSTING_LIST_VEC_H

#include <string>
#include <string.h>
#include <vector>

#include <glog/logging.h>

#include "engine_services.h"
#include "posting_basic.h"


// Not inherit any class yet... Need to re-think the interface
// after I get more info.
template <class T>
class PostingList_Vec {
 private:
  typedef std::vector<T> PostingStore;

  Term term_;                          // term this posting list belongs to
  PostingStore posting_store_;         

 public:
  // Note that this is not the same as STL iterators
  typedef int iterator_t;


  PostingList_Vec(const std::string &term)
    :term_(term) {
  }

  Term GetTerm() {return term_;}
  std::size_t Size() {return posting_store_.size();}

  // You must make sure postings are added with increasing doc ID
  void AddPosting(const Posting &posting) {
    if (posting_store_.size() > 0 && 
        posting_store_.back().GetDocId() >= posting.GetDocId()) {
      throw std::runtime_error(
          "New posting doc ID must be larger than the last existing one.");
    }
    posting_store_.push_back(posting);
  }

  const T& GetPosting(const iterator_t &it) {
    return posting_store_.at(it);
  }

  const iterator_t SkipForward(const iterator_t &it, const DocIdType &doc_id) {
    // return an iterator that has doc ID that is larger or equal to doc_id
    // It returns n (index past the last posting) if we could not find such a doc ID
    iterator_t i = it;
    const std::size_t n = posting_store_.size();

    while (GetPosting(i).GetDocId() < doc_id) {
      i++;
      if ( i >= n ) {
        return n;
      }
    }

    return i;
  }

};


#endif
