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
  PostingList_Vec(const std::string &term)
    :term_(term) {
  }

  Term GetTerm() {return term_;}
  std::size_t Size() {return posting_store_.size();}

  // You must make sure postings are added with increasing doc ID
  void AddPosting(const Posting &posting) {
    if (posting_store_.size() > 0 && 
        posting_store_.back().GetDocID() >= posting.GetDocID()) {
      throw std::runtime_error(
          "New posting doc ID must be larger than the last existing one.");
    }
    posting_store_.push_back(posting);
  }

  const T& GetPosting(const int &index) {
    return posting_store_.at(index);
  }

};


#endif
