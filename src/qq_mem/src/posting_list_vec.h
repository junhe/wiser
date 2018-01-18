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
  const int skip_span_;

 public:
  // Note that this is not the same as STL iterators
  typedef int iterator_t;

  PostingList_Vec(const std::string &term)
    :term_(term), skip_span_(100) {
  }

  PostingList_Vec(const std::string &term, const int &skip_span)
    :term_(term), skip_span_(skip_span) {
  }

  const bool HasSkip(const iterator_t &it) const {
    return it % skip_span_ == 0 && it + skip_span_ < posting_store_.size();
  }
  const int GetSkipSpan() const { return skip_span_; }
  const Term GetTerm() const {return term_;}
  std::size_t Size() const {return posting_store_.size();}

  iterator_t cbegin() const {return 0;}
  iterator_t cend() const {return posting_store_.size();}

  // You must make sure postings are added with increasing doc ID
  void AddPosting(const T &posting) {
    if (posting_store_.size() > 0 && 
        posting_store_.back().GetDocId() >= posting.GetDocId()) {
      std::cout << posting_store_.back().GetDocId() << ">=" << posting.GetDocId() << std::endl;
      //std::cout << term_ << std::endl;
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

    while (i < n) {
      // Worth skipping? (everything in the next span is less than doc_id)
      if (HasSkip(i) && GetPosting(i + skip_span_).GetDocId() <= doc_id) {
        i += skip_span_;
        continue;
      }

      // Check i's doc id
      if (GetPosting(i).GetDocId() >= doc_id) {
        return i; 
      }

      i++;
    }

    return i;
  }

};


#endif
