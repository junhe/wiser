#ifndef QUERY_PROCESSING_H
#define QUERY_PROCESSING_H

#include <vector>
#include <cassert>
#include <unordered_map>
#include <queue>

#define NDEBUG

#include <glog/logging.h>

#include "doc_length_store.h"
#include "engine_services.h"
#include "posting_list_vec.h"
#include "posting_list_delta.h"
#include "scoring.h"
#include "utils.h"


typedef std::vector<PostingListDeltaIterator> PostingListIterators;
typedef std::vector<std::unique_ptr<PopIteratorService>> PositionIterators;
typedef std::vector<PopIteratorService *> PositionIterators2;

struct PositionInfo {
  int pos = 0;
  //                   hello readers, this is my hello world program
  // term_appearance   0                         1
  int term_appearance = 0;
};

typedef std::vector<PositionInfo> PositionInfoVec;
typedef std::vector<PositionInfoVec> PositionInfoTable;

typedef std::vector<std::shared_ptr<OffsetPairsIteratorService>> OffsetIterators;


class PhraseQueryProcessor2 {
 public:
  // Order matters in iterators. If "hello world" is 
  // the phrase query, iterator for "hello" should 
  // be the first iterator in iterators;
  PhraseQueryProcessor2(PositionIterators2 *iterators)
    :iterators_(iterators), last_orig_popped_(iterators->size()) {
  }

  PhraseQueryProcessor2()
    :iterators_(nullptr), last_orig_popped_() {
  }

  void Reset(PositionIterators2 *iterators) {
    iterators_ = iterators;
    last_orig_popped_.resize(iterators->size());
  }

  Position FindMaxAdjustedLastPopped() {
    Position max = 0;
    Position adjusted_pos;

    // original - i = adjusted
    //
    //      hello world program
    // orig:    0     1       2
    // adj:     0     0       0
    //
    // if the adjusted pos are the same, it is a phrase match
    for(int i = 0; i < last_orig_popped_.size(); i++) {
      adjusted_pos = last_orig_popped_[i].pos - i;
      if (adjusted_pos > max) {
        max = adjusted_pos;
      }
    }
    return max;
  }

  // Return false if any of the lists has no pos larger than or equal to
  // max_adjusted_pos
  bool MovePoppedBeyond(Position max_adjusted_pos) {
    bool ret;
    for (int i = 0; i < iterators_->size(); i++) {
      ret = MovePoppedBeyond(i, max_adjusted_pos);
      if (ret == false) {
        return false;
      }
    }

    return true;
  }

  bool MovePoppedBeyond(int i, Position max_adjusted_pos) {
    // the last popped will be larger than or equal to max_pos
    // If the last popped is larger than or equal to max_adjusted_pos
    auto &it = (*iterators_)[i];

    while (it->IsEnd() == false && last_orig_popped_[i].pos - i < max_adjusted_pos) {
      last_orig_popped_[i].pos = it->Pop();
      last_orig_popped_[i].term_appearance++;
    }
    
    if (it->IsEnd() == true && last_orig_popped_[i].pos - i < max_adjusted_pos) {
      return false;
    } else {
      return true;
    }
  }

  bool IsPoppedMatch(Position max_adjusted_pos) {
    Position adjusted_pos;

    for(int i = 0; i < last_orig_popped_.size(); i++) {
      adjusted_pos = last_orig_popped_[i].pos - i;
      if (adjusted_pos != max_adjusted_pos) {
        return false;
      }
    }
    return true;
  }

  bool InitializeLastPopped() {
    for (int i = 0; i < last_orig_popped_.size(); i++) {
      if ((*iterators_)[i]->IsEnd()) {
        // one list is empty
        return false;
      }
      last_orig_popped_[i].pos = (*iterators_)[i]->Pop();
      last_orig_popped_[i].term_appearance = 0;
    }
    return true;
  }

  //          --- table ---
  // term01: info_col  info_col ...
  // term02: info_col  info_col ...
  // term03: info_col  info_col ...
  // ...
  void AppendPositionCol(PositionInfoTable *table) {
    for (int i = 0; i < last_orig_popped_.size(); i++) {
      PositionInfo info = last_orig_popped_[i];
      PositionInfoVec &row = (*table)[i];
      row.push_back(info);
    }
  }

  PositionInfoTable Process() {
    if ((*iterators_).size() == 2) {
      return ProcessTwoTerm();
    } else {
      return ProcessGeneral();
    }
  }

  PositionInfoTable ProcessTwoTerm() {
    PositionInfoTable ret_table((*iterators_).size()); 

    PopIteratorService *it0 = (*iterators_)[0]; 
    PopIteratorService *it1 = (*iterators_)[1]; 
    int pos0, pos1;
    int apr0 = -1, apr1 = -1;

    pos0 = -100;
    pos1 = -200;

    // loop inv: 
    //   iterator points to first unpoped item
    //   pos is the last poped (it - 1). Pos is adjusted
    //   everything before pos has been checked
    //   apr is the appearance of the iterator
    bool tried_pop_end = false;
    while(tried_pop_end == false) {
      if (pos0 < pos1) {
        if (it0->IsEnd() == false) {
          pos0 = it0->Pop();  
          apr0++;
        } else {
          tried_pop_end = true;
        }
      } else if (pos0 > pos1) {
        if (it1->IsEnd() == false) { // add test for it0
          pos1 = it1->Pop() - 1;  
          apr1++;
        } else {
          tried_pop_end = true;
        }
      } else {
        PositionInfo info0;
        info0.pos = pos0;
        info0.term_appearance = apr0;
        ret_table[0].push_back(info0);

        PositionInfo info1;
        info1.pos = pos1 + 1;
        info1.term_appearance = apr1;
        ret_table[1].push_back(info1);

        if (it0->IsEnd() == false) {
          pos0 = it0->Pop();  
          apr0++;
        } else {
          tried_pop_end = true;
        }

        if (it1->IsEnd() == false) {
          pos1 = it1->Pop() - 1;  
          apr1++;
        } else {
          tried_pop_end = true;
        }
      }
    }

    return ret_table;
  }

  PositionInfoTable ProcessGeneral() {
    bool any_list_exhausted = false;
    PositionInfoTable ret_table((*iterators_).size()); 

    if (InitializeLastPopped() == false) {
      return ret_table;
    }

    while (any_list_exhausted == false) {
      Position max_adjusted_pos = FindMaxAdjustedLastPopped(); 
      bool found = MovePoppedBeyond(max_adjusted_pos);

      if (found == false) {
        any_list_exhausted = true;
        continue;
      } 

      bool match = IsPoppedMatch(max_adjusted_pos);        
      if (match == true) {
        AppendPositionCol(&ret_table);

        bool found = MovePoppedBeyond(max_adjusted_pos + 1);
        if (found == false) {
          any_list_exhausted = true;
        }
      }
    }

    return ret_table;
  }

 private:
  PositionIterators2 *iterators_;
  // We need signed int because adjusted pos can be negative
  std::vector<PositionInfo> last_orig_popped_;
  bool list_exhausted_ = false;
};


class PhraseQueryProcessor3 {
 public:
  PhraseQueryProcessor3()
    :solid_iterators_(3), last_orig_popped_(3) {
  }

  Position FindMaxAdjustedLastPopped() {
    Position max = 0;
    Position adjusted_pos;

    // original - i = adjusted
    //
    //      hello world program
    // orig:    0     1       2
    // adj:     0     0       0
    //
    // if the adjusted pos are the same, it is a phrase match
    for(int i = 0; i < last_orig_popped_.size(); i++) {
      adjusted_pos = last_orig_popped_[i].pos - i;
      if (adjusted_pos > max) {
        max = adjusted_pos;
      }
    }
    return max;
  }

  // Return false if any of the lists has no pos larger than or equal to
  // max_adjusted_pos
  bool MovePoppedBeyond(Position max_adjusted_pos) {
    bool ret;
    for (int i = 0; i < solid_iterators_.size(); i++) {
      ret = MovePoppedBeyond(i, max_adjusted_pos);
      if (ret == false) {
        return false;
      }
    }

    return true;
  }

  bool MovePoppedBeyond(int i, Position max_adjusted_pos) {
    // the last popped will be larger than or equal to max_pos
    // If the last popped is larger than or equal to max_adjusted_pos
    auto &it = solid_iterators_[i];

    while (it.IsEnd() == false && last_orig_popped_[i].pos - i < max_adjusted_pos) {
      last_orig_popped_[i].pos = it.Pop();
      last_orig_popped_[i].term_appearance++;
    }
    
    if (it.IsEnd() == true && last_orig_popped_[i].pos - i < max_adjusted_pos) {
      return false;
    } else {
      return true;
    }
  }

  bool IsPoppedMatch(Position max_adjusted_pos) {
    Position adjusted_pos;

    for(int i = 0; i < last_orig_popped_.size(); i++) {
      adjusted_pos = last_orig_popped_[i].pos - i;
      if (adjusted_pos != max_adjusted_pos) {
        return false;
      }
    }
    return true;
  }

  bool InitializeLastPopped() {
    for (int i = 0; i < last_orig_popped_.size(); i++) {
      if (solid_iterators_[i].IsEnd()) {
        // one list is empty
        return false;
      }
      last_orig_popped_[i].pos = solid_iterators_[i].Pop();
      last_orig_popped_[i].term_appearance = 0;
    }
    return true;
  }

  //          --- table ---
  // term01: info_col  info_col ...
  // term02: info_col  info_col ...
  // term03: info_col  info_col ...
  // ...
  void AppendPositionCol(PositionInfoTable *table) {
    for (int i = 0; i < last_orig_popped_.size(); i++) {
      PositionInfo info = last_orig_popped_[i];
      PositionInfoVec &row = (*table)[i];
      row.push_back(info);
    }
  }

  PositionInfoTable Process() {
    if (n_terms_ == 2) {
      return ProcessTwoTerm();
    } else {
      return ProcessGeneral();
    }
  }

  PositionInfoTable ProcessTwoTerm() {
    PositionInfoTable ret_table(n_terms_); 

    PopIteratorService *it0 = &solid_iterators_[0]; 
    PopIteratorService *it1 = &solid_iterators_[1];
    int pos0, pos1;
    int apr0 = -1, apr1 = -1;

    pos0 = -100;
    pos1 = -200;

    // loop inv: 
    //   iterator points to first unpoped item
    //   pos is the last poped (it - 1). Pos is adjusted
    //   everything before pos has been checked
    //   apr is the appearance of the iterator
    bool tried_pop_end = false;
    while(tried_pop_end == false) {
      if (pos0 < pos1) {
        if (it0->IsEnd() == false) {
          pos0 = it0->Pop();  
          apr0++;
        } else {
          tried_pop_end = true;
        }
      } else if (pos0 > pos1) {
        if (it1->IsEnd() == false) { // add test for it0
          pos1 = it1->Pop() - 1;  
          apr1++;
        } else {
          tried_pop_end = true;
        }
      } else {
        PositionInfo info0;
        info0.pos = pos0;
        info0.term_appearance = apr0;
        ret_table[0].push_back(info0);

        PositionInfo info1;
        info1.pos = pos1 + 1;
        info1.term_appearance = apr1;
        ret_table[1].push_back(info1);

        if (it0->IsEnd() == false) {
          pos0 = it0->Pop();  
          apr0++;
        } else {
          tried_pop_end = true;
        }

        if (it1->IsEnd() == false) {
          pos1 = it1->Pop() - 1;  
          apr1++;
        } else {
          tried_pop_end = true;
        }
      }
    }

    return ret_table;
  }

  PositionInfoTable ProcessGeneral() {
    bool any_list_exhausted = false;
    PositionInfoTable ret_table(n_terms_); 

    if (InitializeLastPopped() == false) {
      return ret_table;
    }

    while (any_list_exhausted == false) {
      Position max_adjusted_pos = FindMaxAdjustedLastPopped(); 
      bool found = MovePoppedBeyond(max_adjusted_pos);

      if (found == false) {
        any_list_exhausted = true;
        continue;
      } 

      bool match = IsPoppedMatch(max_adjusted_pos);        
      if (match == true) {
        AppendPositionCol(&ret_table);

        bool found = MovePoppedBeyond(max_adjusted_pos + 1);
        if (found == false) {
          any_list_exhausted = true;
        }
      }
    }

    return ret_table;
  }

  CompressedPositionIterator *Iterator(int i) {
    return &solid_iterators_[i];
  }

  void SetNumTerms(int n) {
    n_terms_ = n;
  }

 private:
  int n_terms_;
  std::vector<CompressedPositionIterator> solid_iterators_;
  std::vector<PositionInfo> last_orig_popped_;
  bool list_exhausted_ = false;
};



class PhraseQueryProcessor {
 public:
  // Order matters in iterators. If "hello world" is 
  // the phrase query, iterator for "hello" should 
  // be the first iterator in iterators;
  PhraseQueryProcessor(PositionIterators *iterators)
    :iterators_(*iterators), last_orig_popped_(iterators->size()) {
  }

  Position FindMaxAdjustedLastPopped() {
    Position max = 0;
    Position adjusted_pos;

    // original - i = adjusted
    //
    //      hello world program
    // orig:    0     1       2
    // adj:     0     0       0
    //
    // if the adjusted pos are the same, it is a phrase match
    for(int i = 0; i < last_orig_popped_.size(); i++) {
      adjusted_pos = last_orig_popped_[i].pos - i;
      if (adjusted_pos > max) {
        max = adjusted_pos;
      }
    }
    return max;
  }

  // Return false if any of the lists has no pos larger than or equal to
  // max_adjusted_pos
  bool MovePoppedBeyond(Position max_adjusted_pos) {
    bool ret;
    for (int i = 0; i < iterators_.size(); i++) {
      ret = MovePoppedBeyond(i, max_adjusted_pos);
      if (ret == false) {
        return false;
      }
    }

    return true;
  }

  bool MovePoppedBeyond(int i, Position max_adjusted_pos) {
    // the last popped will be larger than or equal to max_pos
    // If the last popped is larger than or equal to max_adjusted_pos
    auto &it = iterators_[i];

    while (it->IsEnd() == false && last_orig_popped_[i].pos - i < max_adjusted_pos) {
      last_orig_popped_[i].pos = it->Pop();
      last_orig_popped_[i].term_appearance++;
    }
    
    if (it->IsEnd() == true && last_orig_popped_[i].pos - i < max_adjusted_pos) {
      return false;
    } else {
      return true;
    }
  }

  bool IsPoppedMatch(Position max_adjusted_pos) {
    Position adjusted_pos;

    for(int i = 0; i < last_orig_popped_.size(); i++) {
      adjusted_pos = last_orig_popped_[i].pos - i;
      if (adjusted_pos != max_adjusted_pos) {
        return false;
      }
    }
    return true;
  }

  bool InitializeLastPopped() {
    for (int i = 0; i < last_orig_popped_.size(); i++) {
      if (iterators_[i]->IsEnd()) {
        // one list is empty
        return false;
      }
      last_orig_popped_[i].pos = iterators_[i]->Pop();
      last_orig_popped_[i].term_appearance = 0;
    }
    return true;
  }

  //          --- table ---
  // term01: info_col  info_col ...
  // term02: info_col  info_col ...
  // term03: info_col  info_col ...
  // ...
  void AppendPositionCol(PositionInfoTable *table) {
    for (int i = 0; i < last_orig_popped_.size(); i++) {
      PositionInfo info = last_orig_popped_[i];
      PositionInfoVec &row = (*table)[i];
      row.push_back(info);
    }
  }

  PositionInfoTable Process() {
    if (iterators_.size() == 2) {
      return ProcessTwoTerm();
    } else {
      return ProcessGeneral();
    }
  }

  PositionInfoTable ProcessTwoTerm() {
    PositionInfoTable ret_table(iterators_.size()); 

    PopIteratorService *it0 = iterators_[0].get(); 
    PopIteratorService *it1 = iterators_[1].get(); 
    int pos0, pos1;
    int apr0 = -1, apr1 = -1;

    pos0 = -100;
    pos1 = -200;

    // loop inv: 
    //   iterator points to first unpoped item
    //   pos is the last poped (it - 1). Pos is adjusted
    //   everything before pos has been checked
    //   apr is the appearance of the iterator
    bool tried_pop_end = false;
    while(tried_pop_end == false) {
      if (pos0 < pos1) {
        if (it0->IsEnd() == false) {
          pos0 = it0->Pop();  
          apr0++;
        } else {
          tried_pop_end = true;
        }
      } else if (pos0 > pos1) {
        if (it1->IsEnd() == false) { // add test for it0
          pos1 = it1->Pop() - 1;  
          apr1++;
        } else {
          tried_pop_end = true;
        }
      } else {
        PositionInfo info0;
        info0.pos = pos0;
        info0.term_appearance = apr0;
        ret_table[0].push_back(info0);

        PositionInfo info1;
        info1.pos = pos1 + 1;
        info1.term_appearance = apr1;
        ret_table[1].push_back(info1);

        if (it0->IsEnd() == false) {
          pos0 = it0->Pop();  
          apr0++;
        } else {
          tried_pop_end = true;
        }

        if (it1->IsEnd() == false) {
          pos1 = it1->Pop() - 1;  
          apr1++;
        } else {
          tried_pop_end = true;
        }
      }
    }

    return ret_table;
  }

  PositionInfoTable ProcessGeneral() {
    bool any_list_exhausted = false;
    PositionInfoTable ret_table(iterators_.size()); 

    if (InitializeLastPopped() == false) {
      return ret_table;
    }

    while (any_list_exhausted == false) {
      Position max_adjusted_pos = FindMaxAdjustedLastPopped(); 
      bool found = MovePoppedBeyond(max_adjusted_pos);

      if (found == false) {
        any_list_exhausted = true;
        continue;
      } 

      bool match = IsPoppedMatch(max_adjusted_pos);        
      if (match == true) {
        AppendPositionCol(&ret_table);

        bool found = MovePoppedBeyond(max_adjusted_pos + 1);
        if (found == false) {
          any_list_exhausted = true;
        }
      }
    }

    return ret_table;
  }

 private:
  PositionIterators &iterators_;
  // We need signed int because adjusted pos can be negative
  std::vector<PositionInfo> last_orig_popped_;
  bool list_exhausted_ = false;
};


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
    const std::vector<const PostingList_Vec<T>*> &lists, IntersectionResult *res=nullptr) {
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



// This is the score of a document for a query. This query may have multiple
// terms.
//
// Input:
//   postings is a set of postings
//         term0        term1
//    doc  posting[0]   posting[1]
//
//   doc_freqs_of_terms: number of docs that have a term
//         term0        term1
//         doc_freq[0]   doc_freq[1]
//
// Note that lists[i], posting_iters[i] and doc_freqs_of_terms[i] must 
// for the same term.
// All posting_iters[] must point to postings of the same document.
//
// T is a class for a posting.
template <class T>
inline qq_float calc_doc_score_for_a_query(
    const std::vector<const PostingList_Vec<T>*> &lists, 
    const std::vector<typename PostingList_Vec<T>::iterator_t> &posting_iters,
    // const std::vector<int> &doc_freqs_of_terms,
    const std::vector<qq_float> &idfs_of_terms,
    const int &n_total_docs_in_index, 
    const qq_float &avg_doc_length_in_index,
    const int &length_of_this_doc) 
{
  qq_float final_doc_score = 0;

  for (int list_i = 0; list_i < lists.size(); list_i++) {
    // calc score of a term in one loop

    // get term freq
    const PostingList_Vec<T> *postinglist = lists[list_i];
    const typename PostingList_Vec<T>::iterator_t it = posting_iters[list_i];
    const int cur_term_freq = postinglist->GetPosting(it).GetTermFreq(); 

    // get the number of documents that have the current term
    // const int doc_freq = doc_freqs_of_terms[list_i];

    // qq_float idf = calc_es_idf(n_total_docs_in_index, doc_freq);
    qq_float idf = idfs_of_terms[list_i];
    qq_float tfnorm = calc_es_tfnorm(cur_term_freq, length_of_this_doc, 
        avg_doc_length_in_index);

    // ES 6.1 uses tfnorm * idf 
    qq_float term_doc_score = idf * tfnorm;

    final_doc_score += term_doc_score;
  }

  return final_doc_score;
}


qq_float calc_doc_score_for_a_query(
    const IteratorPointers &pl_iterators,
    const std::vector<qq_float> &idfs_of_terms,
    const int &n_total_docs_in_index, 
    const qq_float &avg_doc_length_in_index,
    const int &length_of_this_doc);


struct ResultDocEntry {
  DocIdType doc_id;
  qq_float score;
  std::vector<StandardPosting> postings;
  Positions phrase_positions;
  // table[0]: row of position info structs for the first term
  // table[1]: row of position info structs for the second term
  PositionInfoTable position_table;
  // offset_iters[0]: offset iterator for the first term
  // offset_iters[1]: offset iterator for the second term
  // ...
  OffsetIterators offset_iters;
  bool is_phrase = false;

  ResultDocEntry(const DocIdType &doc_id_in, const qq_float &score_in)
    :doc_id(doc_id_in), score(score_in) {}

  ResultDocEntry(const DocIdType &doc_id_in, 
                 const qq_float &score_in, 
                 const OffsetIterators offset_iters_in, 
                 const PositionInfoTable position_table_in, 
                 const bool is_phrase_in)
    :doc_id(doc_id_in), 
     score(score_in), 
     offset_iters(offset_iters_in),
     position_table(position_table_in), 
     is_phrase(is_phrase_in) {}

  std::vector<OffsetPairs> OffsetsForHighliting() {
    if (is_phrase == true) {
      return FilterOffsetByPosition();
    } else {
      return ExpandOffsets();
    }
  }

  std::vector<OffsetPairs> ExpandOffsets() {
    std::vector<OffsetPairs> table(offset_iters.size());
    for (int i = 0; i < offset_iters.size(); i++) {
      auto &it = offset_iters[i];
      while (it->IsEnd() == false) {
        OffsetPair pair;
        it->Pop(&pair);
        table[i].push_back(pair);
      }
    }
    return table;
  }

  // Make sure you have valid offset and positions in this object
  // before calling this function
  std::vector<OffsetPairs> FilterOffsetByPosition() {
    std::vector<OffsetPairs> offset_table(position_table.size());

    for (int row_i = 0; row_i < position_table.size(); row_i++) {
      auto offset_iter = offset_iters[row_i];
      const PositionInfoVec &row = position_table[row_i];

      int offset_cur = -1;
      for (int col_i = 0; col_i < row.size(); col_i++) {
        OffsetPair pair;
        while (offset_cur < row[col_i].term_appearance) {
          // assert(offset_iter->IsEnd() == false);
          LOG_IF(FATAL, offset_iter->IsEnd()) 
            << "offset_iter does not suppose to reach the end." << std::endl;
          offset_iter->Pop(&pair);
          offset_cur++;
        }

        offset_table[row_i].push_back(pair);
      }
    }

    return offset_table;
  }

  friend bool operator<(const ResultDocEntry &a, const ResultDocEntry &b)
  {
    return a.score < b.score;
  }

  friend bool operator>(const ResultDocEntry &a, const ResultDocEntry &b)
  {
    return a.score > b.score;
  }

  std::string ToStr() {
    return std::to_string(doc_id) + " (" + std::to_string(score) + ")";
  }
};


typedef std::priority_queue<ResultDocEntry, std::vector<ResultDocEntry>, 
    std::greater<ResultDocEntry> > MinHeap;

class QueryProcessor {
 public:
  QueryProcessor(
    CompressedPositionIteratorPool * position_iterator_pool,
    IteratorPointers *pl_iterators, 
    const DocLengthStore &doc_lengths,
    const int n_total_docs_in_index,
    const int k = 5,
    const bool is_phrase = false)
  : position_iterator_pool_(position_iterator_pool),
    n_lists_(pl_iterators->size()),
    doc_lengths_(doc_lengths),
    pl_iterators_(*pl_iterators),
    k_(k),
    is_phrase_(is_phrase),
    idfs_of_terms_(n_lists_),
    n_total_docs_in_index_(n_total_docs_in_index)
  {
    for (int i = 0; i < n_lists_; i++) {
      idfs_of_terms_[i] = calc_es_idf(n_total_docs_in_index_, 
                                      pl_iterators_[i]->Size());
    }
  }

  std::vector<ResultDocEntry> Process() {
    if (pl_iterators_.size() == 1) {
      return ProcessSingleTerm();
    } else if (pl_iterators_.size() == 2) {
      return ProcessTwoTerm();
    } else {
      return ProcessMultipleTerms();
    }
  }

  std::vector<ResultDocEntry> ProcessMultipleTerms() {
    bool finished = false;
    DocIdType max_doc_id;

    // loop inv: all intersections before iterators have been found
    while (finished == false) {
      // find max doc id
      max_doc_id = -1;
      finished = FindMax(&max_doc_id);

      if (finished == true) {
        break;
      }

      finished = FindMatch(max_doc_id);
    } // while

    return SortHeap();
  }

  std::vector<ResultDocEntry> ProcessSingleTerm() {
    auto &it = pl_iterators_[0];
    PositionInfoTable position_table(1);

    while (it->IsEnd() == false) {
      DocIdType doc_id = it->DocId();
      RankDoc(doc_id, position_table);
      it->Advance();
    }

    return SortHeap();
  }

  std::vector<ResultDocEntry> ProcessTwoTerm() {
    auto &it_0 = pl_iterators_[0];
    auto &it_1 = pl_iterators_[1];
    DocIdType doc0, doc1;

    while (it_0->IsEnd() == false && it_1->IsEnd() == false) {
      doc0 = it_0->DocId();
      doc1 = it_1->DocId();
      if (doc0 > doc1) {
        it_1->SkipForward(doc0);
      } else if (doc0 < doc1) {
        it_0->SkipForward(doc1);
      } else {
        HandleTheFoundDoc(doc0); 

        it_0->Advance();
        it_1->Advance();
      }
    }

    return SortHeap();
  }

 private:
  // return true: end reached
  bool FindMax(DocIdType * max_doc_id) {
    for (int list_i = 0; list_i < n_lists_; list_i++) {
      auto it = pl_iterators_[list_i].get();

      if (it->IsEnd()) {
        return true;
      }

      const DocIdType cur_doc_id = it->DocId(); 
      if (cur_doc_id > *max_doc_id) {
        *max_doc_id = cur_doc_id; 
      }
    }
    return false;
  }

  // return true: end reached
  bool FindMatch(DocIdType max_doc_id) {
    // Try to reach max_doc_id in all posting lists_
    int list_i;
    for (list_i = 0; list_i < n_lists_; list_i++) {
      auto it = pl_iterators_[list_i].get();

      it->SkipForward(max_doc_id);
      if (it->IsEnd()) {
        return true;
      }

      if (it->DocId() != max_doc_id) {
        break;
      }

      if (list_i == n_lists_ - 1) {
        // HandleTheFoundDoc(max_doc_id);
        HandleTheFoundDoc(max_doc_id);
        // Advance iterators
        for (int i = 0; i < n_lists_; i++) {
          pl_iterators_[i]->Advance();
        }
      }
    }
    return false;
  }

  PositionInfoTable FindPhraseOLD() {
    // All iterators point to the same posting at this point
    PositionIterators iterators;
    for (int i = 0; i < pl_iterators_.size(); i++) {
      iterators.push_back(std::move(pl_iterators_[i]->PositionBegin()));
    }

    PhraseQueryProcessor phrase_qp(&iterators);
    return phrase_qp.Process();
  }

  PositionInfoTable FindPhrase() {
    PositionIterators2 iterators;
    for (int i = 0; i < pl_iterators_.size(); i++) {
      CompressedPositionIterator *p = position_iterator_pool_->Borrow(i);
      pl_iterators_[i]->AssignPositionBegin(p);
      iterators.push_back(p);
    }

    phrase_qp_.Reset(&iterators);
    return phrase_qp_.Process();
  }

  void HandleTheFoundDoc(const DocIdType &max_doc_id) {
    if (is_phrase_ == true && pl_iterators_.size() > 1 ) {
      auto position_table = FindPhrase();
      if (position_table[0].size() > 0) {
        RankDoc(max_doc_id, position_table);
      }
    } else {
      PositionInfoTable position_table(pl_iterators_.size());
      RankDoc(max_doc_id, position_table);
    }
  }

  void RankDoc(const DocIdType &max_doc_id, const PositionInfoTable &position_table) {
    qq_float score_of_this_doc = calc_doc_score_for_a_query(
        pl_iterators_,
        idfs_of_terms_,
        n_total_docs_in_index_,
        doc_lengths_.GetAvgLength(),
        doc_lengths_.GetLength(max_doc_id));

    if (min_heap_.size() < k_) {
      InsertToHeap(max_doc_id, score_of_this_doc, position_table);
    } else {
      if (score_of_this_doc > min_heap_.top().score) {
        min_heap_.pop();
        InsertToHeap(max_doc_id, score_of_this_doc, position_table);
      }
    }
  }

  std::vector<ResultDocEntry> SortHeap() {
    std::vector<ResultDocEntry> ret;

    int kk = k_;
    while(!min_heap_.empty() && kk != 0) {
      ret.push_back(min_heap_.top());
      min_heap_.pop();
      kk--;
    }
    std::reverse(ret.begin(), ret.end());
    return ret;
  }

  void InsertToHeap(const DocIdType &doc_id, 
                    const qq_float &score_of_this_doc,
                    const PositionInfoTable &position_table)
  {
    OffsetIterators offset_iters;
    for (int i = 0; i < n_lists_; i++) {
      auto p = pl_iterators_[i]->OffsetPairsBegin();
      offset_iters.push_back(std::move(p));
    }
    min_heap_.emplace(doc_id, score_of_this_doc, offset_iters, position_table, is_phrase_);
  }

  const int n_lists_;
  IteratorPointers &pl_iterators_;
  const int k_;
  const int n_total_docs_in_index_;
  std::vector<qq_float> idfs_of_terms_;
  MinHeap min_heap_;
  const DocLengthStore &doc_lengths_;
  bool is_phrase_;

  CompressedPositionIteratorPool * position_iterator_pool_ = nullptr;
  PhraseQueryProcessor2 phrase_qp_;
  PhraseQueryProcessor3 phrase_qp_3_;
};


namespace qq_search {
std::vector<ResultDocEntry> ProcessQuery(
   CompressedPositionIteratorPool *position_iterator_pool,
   IteratorPointers *pl_iterators, 
   const DocLengthStore &doc_lengths,
   const int n_total_docs_in_index,
   const int k,
   const bool is_phrase);
}


#endif
