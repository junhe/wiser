#ifndef INTERSECT_H
#define INTERSECT_H

#include <vector>
#include <cassert>
#include <unordered_map>
#include <queue>

#define NDEBUG

#include <glog/logging.h>

#include "doc_length_store.h"
#include "engine_services.h"
#include "posting_list_vec.h"
#include "ranking.h"


class IntersectionResult {
 public:
  typedef std::map<Term, const QqMemPostingService *> row_dict_t; 
  typedef std::map<DocIdType, row_dict_t> table_dict_t;

  typedef table_dict_t::const_iterator row_iterator;
  typedef row_dict_t::const_iterator col_iterator;

  void SetPosting(const DocIdType &doc_id, 
                  const Term &term, 
                  const QqMemPostingService* posting) {
    table_[doc_id][term] = posting;
  }

  static DocIdType GetCurDocId(row_iterator &it) {
    return it->first;
  }

  row_iterator row_cbegin() const {
    return table_.cbegin();
  }

  row_iterator row_cend() const {
    return table_.cend();
  }

  const row_dict_t *GetRow(const DocIdType &doc_id) {
    return &table_[doc_id];
  }

  static Term GetCurTerm(col_iterator &it) {
    return it->first;
  }

  const QqMemPostingService *GetPosting(const DocIdType &doc_id, const Term &term) {
    return table_[doc_id][term]; 
  }

  static const QqMemPostingService *GetPosting(col_iterator &it) {
    return it->second;
  }

  static col_iterator col_cbegin(row_iterator &it) {
    return it->second.cbegin(); 
  }

  static col_iterator col_cend(row_iterator &it) {
    return it->second.cend(); 
  }

  void SetDocCount(const Term &term, const int &value) {
    doc_cnt_[term] = value;
  }

  int GetDocCount(const Term &term) const {
    return doc_cnt_.at(term);
  }

  std::size_t Size() {
    return table_.size();
  }

 private:
  typedef std::map<Term, int> doc_cnt_dict_t;

  table_dict_t table_;
  doc_cnt_dict_t doc_cnt_;
};



TermScoreMap score_terms_in_doc(const IntersectionResult &intersection_result, 
    IntersectionResult::row_iterator row_it,
    const qq_float &avg_doc_length, 
    const int &doc_length,
    const int &total_docs);

qq_float aggregate_term_score(const TermScoreMap &term_scores);

DocScoreVec score_docs(const IntersectionResult &intersection_result, 
                       const DocLengthStore &doc_lengths);

// lists is a vector of pointers, pointing to posting lists
// TODO: This function is too long, refactor it.
//
// Requirements for template class T:
//  - class T must have member function const DocIdType T::GetDocId() const.
//    This is the requirement imposed by PostingList_Vec.
//  - If res != nullptr, T must has T::GetTermFreq() const
template <class T>
std::vector<DocIdType> intersect(
    const std::vector<const PostingList_Vec<T>*> lists, IntersectionResult *res=nullptr) {
  const int n_lists = lists.size();
  std::vector<typename PostingList_Vec<T>::iterator_t> posting_iters(n_lists);
  std::vector<DocIdType> ret_vec{};
  bool finished = false;
  DocIdType max_doc_id = -1;
  
  for (int list_i = 0; list_i < n_lists; list_i++) {
    posting_iters[list_i] = 0;
  }

  while (finished == false) {
    // find max doc id
    max_doc_id = -1;
    for (int list_i = 0; list_i < n_lists; list_i++) {
      const PostingList_Vec<T> *postinglist = lists[list_i];
      typename PostingList_Vec<T>::iterator_t it = posting_iters[list_i];
      if (it == postinglist->Size()) {
        finished = true;
        break;
      }
      const DocIdType cur_doc_id = postinglist->GetPosting(it).GetDocId(); 

      if (cur_doc_id > max_doc_id) {
        max_doc_id = cur_doc_id; 
      }
    }
    DLOG(INFO) << "max_doc_id: " << max_doc_id << std::endl;

    if (finished == true) {
      break;
    }

    // Try to reach max_doc_id in all posting lists
    int list_i;
    for (list_i = 0; list_i < n_lists; list_i++) {
      const PostingList_Vec<T> *postinglist = lists[list_i];
      typename PostingList_Vec<T>::iterator_t *p_it = &posting_iters[list_i];

      *p_it = postinglist->SkipForward(*p_it, max_doc_id);
      if (*p_it == postinglist->Size()) {
        finished = true;
        break;
      }

      const DocIdType cur_doc_id = postinglist->GetPosting(*p_it).GetDocId(); 
      // (*p_it)++;
      
      if (cur_doc_id != max_doc_id) {
        break;
      }
    }
    
    if (list_i == n_lists) {
      // all posting lists are at max_doc_id 
      ret_vec.push_back(max_doc_id);

      // Get all term frequencies for doc 'max_doc_id'
      if (res != nullptr) {
        for (int i = 0; i < n_lists; i++) {
          const PostingList_Vec<T> *postinglist = lists[i];
          typename PostingList_Vec<T>::iterator_t *p_it = &posting_iters[i];
          const QqMemPostingService *posting = &postinglist->GetPosting(*p_it); 
          res->SetPosting(max_doc_id, postinglist->GetTerm(), posting);
        }
      }

      // Advance iterators
      for (int i = 0; i < n_lists; i++) {
        posting_iters[i]++;
      }
    }

  } // while

  // set doc count of each term
  if (res != nullptr) {
    for (int list_i = 0; list_i < n_lists; list_i++) {
      const PostingList_Vec<T> *postinglist = lists[list_i];
      res->SetDocCount(postinglist->GetTerm(), postinglist->Size());
    }
  }

  return ret_vec;
}





#endif
