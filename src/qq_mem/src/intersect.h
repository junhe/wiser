#ifndef INTERSECT_H
#define INTERSECT_H

#include <vector>
#include <glog/logging.h>
#include <cassert>

#include "engine_services.h"
#include "posting_list_vec.h"
#include "ranking.h"


// TF table
//        term1  term2  term3  term4
// doc1  x     x     x     x
// doc2  x     x     x     x
// doc3  x     x     x     x
//
// IDF dict
//
//         docCount
// term1   x
// term2   x
// term3   x
class TfIdfStore {
 private:
  typedef std::map<Term, int> tf_dict_t;
  typedef std::map<DocIdType, tf_dict_t> tf_table_t;

  typedef std::map<Term, int> doc_cnt_dict_t;

  tf_table_t tf_table_;
  doc_cnt_dict_t doc_cnt_;
 
 public:
  typedef tf_table_t::const_iterator row_iterator;
  typedef tf_dict_t::const_iterator col_iterator;

  DocIdType GetCurDocId(row_iterator &it) const {
    return it->first;
  }

  row_iterator row_cbegin() const {
    return tf_table_.cbegin();
  }

  row_iterator row_cend() const {
    return tf_table_.cend();
  }

  Term GetCurTerm(col_iterator &it) const {
    return it->first;
  }

  int GetCurTermFreq(col_iterator &it) const {
    return it->second;
  }

  col_iterator col_cbegin(row_iterator &it) const {
    return it->second.cbegin(); 
  }

  col_iterator col_cend(row_iterator &it) const {
    return it->second.cend(); 
  }
  
  void SetTf(const DocIdType &doc_id, const Term &term, const int &value) {
    tf_table_[doc_id][term] = value; 
  }

  int GetTf(const DocIdType &doc_id, const Term &term) const {
    return tf_table_.at(doc_id).at(term);
  }

  void SetDocCount(const Term &term, const int &value) {
    doc_cnt_[term] = value;
  }

  int GetDocCount(const Term &term) const {
    return doc_cnt_.at(term);
  }
};


class DocLengthStore {
 private:
  typedef std::unordered_map<DocIdType, int> length_store_t;

  length_store_t length_dict_;
  double avg_length_ = 0;

 public:
  void AddLength(const DocIdType &doc_id, const int &length) {
    if (length_dict_.find(doc_id) != length_dict_.end()) {
      throw std::runtime_error("The requested doc id already exists. "
          "We do not support update.");
    }

    avg_length_ = avg_length_ + (length - avg_length_) / (length_dict_.size() + 1);
    length_dict_[doc_id] = length;
  }

  int GetLength(const DocIdType &doc_id) const {
    return length_dict_.at(doc_id);  
  }

  double GetAvgLength() const {
    return avg_length_;
  }

  std::size_t Size() const {
    return length_dict_.size();
  }
};


// This function iterates the documents in tfidf_sotre and calculate their scores
// You will get one score per document
void score_docs(const TfIdfStore &tfidf_store, const DocLengthStore &doc_lengths) {
  // ES 6.1 uses tfnorm * idf 
  TfIdfStore::row_iterator row_it; 
  TfIdfStore::col_iterator col_it; 

  int avg_doc_length = doc_lengths.GetAvgLength();
  int total_docs = doc_lengths.Size();

  for (row_it = tfidf_store.row_cbegin(); 
       row_it != tfidf_store.row_cend(); 
       row_it++) 
  {
    // each row contains a document.
    DocIdType cur_doc_id = tfidf_store.GetCurDocId(row_it);
    int doc_length = doc_lengths.GetLength(cur_doc_id);

    for (col_it = tfidf_store.col_cbegin(row_it);
         col_it != tfidf_store.col_cend(row_it);
         col_it++) 
    {
      // each column contains a term.
      Term cur_term = tfidf_store.GetCurTerm(col_it);
      int cur_term_freq = tfidf_store.GetCurTermFreq(col_it);
      int doc_freq = tfidf_store.GetDocCount(cur_term);

      double idf = calc_es_idf(total_docs, doc_freq);
      double tfnorm = calc_es_tfnorm(cur_term_freq, doc_length, avg_doc_length);
    }
  }
}



// lists is a vector of pointers, pointing to posting lists
// TODO: This function is too long, refactor it.
//
// Requirements for template class T:
//  - class T must have member function const DocIdType T::GetDocId() const.
//    This is the requirement imposed by PostingList_Vec.
//  - If res != nullptr, T must has T::GetTermFreq() const
template <class T>
std::vector<DocIdType> intersect(
    const std::vector<const PostingList_Vec<T>*> lists, TfIdfStore *res = nullptr) {
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
          const int freq = postinglist->GetPosting(*p_it).GetTermFreq(); 
          res->SetTf(max_doc_id, postinglist->GetTerm(), freq);
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
