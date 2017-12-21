#include "intersect.h"

#include <glog/logging.h>
#include <cassert>


// lists is a vector of pointers, pointing to posting lists
// TODO: This function is too long, refactor it.
std::vector<DocIdType> intersect(
    const std::vector<const PostingList_Vec<Posting>*> lists) {
  const int n_lists = lists.size();
  std::vector<PostingList_Vec<Posting>::iterator_t> posting_iters(n_lists);
  std::vector<DocIdType> ret_vec{};
  
  for (int list_i = 0; list_i < n_lists; list_i++) {
    posting_iters[list_i] = 0;
  }

  bool finished = false;
  while (finished == false) {
    // find max doc id
    DocIdType max_doc_id = -1;
    for (int list_i = 0; list_i < n_lists; list_i++) {
      const PostingList_Vec<Posting> *postinglist = lists[list_i];
      PostingList_Vec<Posting>::iterator_t it = posting_iters[list_i];
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
    for (int list_i = 0; list_i < n_lists; list_i++) {
      const PostingList_Vec<Posting> *postinglist = lists[list_i];
      PostingList_Vec<Posting>::iterator_t *p_it = &posting_iters[list_i];

      *p_it = postinglist->SkipForward(*p_it, max_doc_id);
      if (*p_it == postinglist->Size()) {
        finished = true;
        break;
      }

      const DocIdType cur_doc_id = postinglist->GetPosting(*p_it).GetDocId(); 
      (*p_it)++;
      
      if (cur_doc_id != max_doc_id) {
        break;
      }

      if (list_i == n_lists - 1) {
        // all posting lists are at max_doc_id 
        DLOG(INFO) << "We found one in intersection: " << max_doc_id << std::endl;
        ret_vec.push_back(max_doc_id);
      }
    }
  } // while

  return ret_vec;
}


