#ifndef POSTING_LIST_VEC_H
#define POSTING_LIST_VEC_H

#include <string>
#include <string.h>
#include <vector>

#include "engine_services.h"
#include "posting_basic.h"


// Not inherit any class yet... Need to re-think the interface
// after I get more info.
class PostingList_Vec {
 private:
  typedef std::vector<Posting> PostingStore;

  Term term_;                          // term this posting list belongs to
  PostingStore posting_store_;         

 public:
  PostingList_Vec(const std::string &term);
  Term GetTerm() {return term_;}
  std::size_t Size() {return posting_store_.size();}
  void AddPosting(const Posting &posting);
};


#endif
