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
#include "posting_list_delta.h"
#include "scoring.h"
#include "utils.h"


typedef std::vector<PostingListDeltaIterator> PostingListIterators;


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

  ResultDocEntry(const DocIdType &doc_id_in, const qq_float &score_in)
    :doc_id(doc_id_in), score(score_in) {}

  ResultDocEntry(const DocIdType &doc_id_in, const qq_float &score_in, 
      std::vector<StandardPosting> &postings_in)
    :doc_id(doc_id_in), score(score_in), postings(postings_in) {}

  friend bool operator<(ResultDocEntry a, ResultDocEntry b)
  {
    return a.score < b.score;
  }

  friend bool operator>(ResultDocEntry a, ResultDocEntry b)
  {
    return a.score > b.score;
  }

  std::string ToStr() {
    return std::to_string(doc_id) + " (" + std::to_string(score) + ")";
  }
};



typedef std::priority_queue<ResultDocEntry, std::vector<ResultDocEntry>, 
    std::greater<ResultDocEntry> > MinHeap;
typedef StandardPosting PostingWO;

void insert_to_heap(MinHeap *min_heap, const DocIdType &doc_id, 
    const qq_float &score_of_this_doc, 
    const std::vector<const PostingList_Vec<PostingWO>*> &lists, 
    const std::vector<typename PostingList_Vec<PostingWO>::iterator_t> &posting_iters,
    const int &n_lists);


class QueryProcessor {
 public:
  QueryProcessor(
    IteratorPointers *pl_iterators, 
    const DocLengthStore &doc_lengths,
    const int n_total_docs_in_index,
    const int k = 5)
  : n_lists_(pl_iterators->size()),
    doc_lengths_(doc_lengths),
    pl_iterators_(pl_iterators),
    k_(k),
    idfs_of_terms_(n_lists_),
    n_total_docs_in_index_(n_total_docs_in_index)
  {
    for (int i = 0; i < n_lists_; i++) {
      idfs_of_terms_[i] = calc_es_idf(n_total_docs_in_index_, 
                                      pl_iterators_->at(i)->Size());
    }
  }

  std::vector<ResultDocEntry> Process() {
    bool finished = false;
    DocIdType max_doc_id;

    // loop inv: all intersections before iterators have been found
    while (finished == false) {
      // find max doc id
      max_doc_id = -1;
      for (int list_i = 0; list_i < n_lists_; list_i++) {
        auto it = pl_iterators_->at(list_i).get();

        if (it->IsEnd()) {
          finished = true;
          break;
        }

        const DocIdType cur_doc_id = it->DocId(); 
        if (cur_doc_id > max_doc_id) {
          max_doc_id = cur_doc_id; 
        }
      }

      if (finished == true) {
        break;
      }

      // Try to reach max_doc_id in all posting lists_
      int list_i;
      for (list_i = 0; list_i < n_lists_; list_i++) {
        auto it = pl_iterators_->at(list_i).get();

        it->SkipForward(max_doc_id);
        if (it->IsEnd()) {
          finished = true;
          break;
        }

        if (it->DocId() != max_doc_id) {
          break;
        }

        if (list_i == n_lists_ - 1) {
          HandleTheFoundDoc(max_doc_id);
          // Advance iterators
          for (int i = 0; i < n_lists_; i++) {
            pl_iterators_->at(i)->Advance();
          }
        }
      }
    } // while

    return SortHeap();
  }

 private:
  void HandleTheFoundDoc(const DocIdType &max_doc_id) {
    qq_float score_of_this_doc = calc_doc_score_for_a_query(
        *pl_iterators_,
        idfs_of_terms_,
        n_total_docs_in_index_,
        doc_lengths_.GetAvgLength(),
        doc_lengths_.GetLength(max_doc_id));

    if (min_heap_.size() < k_) {
      InsertToHeap(max_doc_id, score_of_this_doc);
    } else {
      if (score_of_this_doc > min_heap_.top().score) {
        min_heap_.pop();
        InsertToHeap(max_doc_id, score_of_this_doc);
      }
      assert(min_heap_.size() == k_);
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

  void InsertToHeap( const DocIdType &doc_id, const qq_float &score_of_this_doc)
  {
    std::vector<PostingWO> postings;
    for (int i = 0; i < n_lists_; i++) {
      postings.push_back(GetPosting(pl_iterators_->at(i).get()));
    }
    min_heap_.emplace(doc_id, score_of_this_doc, postings);
  }

  StandardPosting GetPosting(const PostingListIteratorService *pl_it) {
    OffsetPairs pairs;
    auto it = pl_it->OffsetPairsBegin();

    while (it->IsEnd() == false) {
      pairs.emplace_back();
      it->Pop(&pairs.back());
    }

    return StandardPosting(pl_it->DocId(), pl_it->TermFreq(), pairs);
  }

  const int n_lists_;
  IteratorPointers *pl_iterators_;
  const int k_;
  const int n_total_docs_in_index_;
  std::vector<qq_float> idfs_of_terms_;
  MinHeap min_heap_;
  const DocLengthStore &doc_lengths_;
};


class VarintIterator {
 public:
  VarintIterator(const std::string *data, const int start_offset, const int count)
    :data_(data), start_offset_(start_offset), cur_offset_(start_offset), 
     count_(count) {}

  VarintIterator(const VarintBuffer &varint_buf, int count)
    :data_(varint_buf.DataPointer()), start_offset_(0), 
     cur_offset_(0), count_(count) {}

  bool IsEnd() {
    return index_ == count_;
  }

  // Must check if it is the end before Pop() is called!
  uint32_t Pop() {
    int len;
    uint32_t n;

    len = utils::varint_decode(*data_, cur_offset_, &n);
    cur_offset_ += len;
    index_++;

    return n;
  }

 private:
  const std::string *data_;
  int cur_offset_; 
  int index_ = 0;
  const int start_offset_;
  const int count_;
};


namespace qq_search {
  std::vector<ResultDocEntry> ProcessQuery(IteratorPointers *pl_iterators, 
                                           const DocLengthStore &doc_lengths,
                                           const int n_total_docs_in_index,
                                           const int k = 5);
}


#endif
