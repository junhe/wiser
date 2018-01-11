#ifndef POSTING_LIST_VEC_H
#define POSTING_LIST_VEC_H

#include <string>
#include <string.h>
#include <vector>

#include <glog/logging.h>

#include "engine_services.h"
#include "posting.h"


// Requirements for class T (a type for posting):
//  - class T must have member function const DocIdType T::GetDocId() const
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

  Term GetTerm() const {return term_;}
  std::size_t Size() const {return posting_store_.size();}

  iterator_t cbegin() const {return 0;}
  iterator_t cend() const {return posting_store_.size();}

  // You must make sure postings are added with increasing doc ID
  void AddPosting(const T &posting) {
    if (posting_store_.size() > 0 && 
        posting_store_.back().GetDocId() >= posting.GetDocId()) {
      throw std::runtime_error(
          "New posting doc ID must be larger than the last existing one.");
    }
    posting_store_.push_back(posting);
  }

  const T& GetPosting(const iterator_t &it) const {
    return posting_store_.at(it);
  }

  const iterator_t SkipForward(const iterator_t &it, const DocIdType &doc_id) const {
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
