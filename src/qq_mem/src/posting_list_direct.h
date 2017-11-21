#ifndef POSTING_LIST_DIRECT_H
#define POSTING_LIST_DIRECT_H

#include <string>
#include <string.h>
#include <vector>
#include "engine_services.h"
#include "posting_basic.h"

// Posting_List Class
class PostingList_Direct :  public PostingListService {
    Term term_;                          // term this posting list belongs to
    std::vector<Posting> p_list;         // posting list when creating index
    
    int num_postings;                    // number of postings in this list
    int cur_index;                       // last posting we have returned
    int cur_docID;                       // last docID we have returned

    public:

// Init with a term string, when creating index
        PostingList_Direct(std::string term_in);    // do we need term?
// Return size
        std::size_t Size();
// Get next posting for query processing
        Posting GetPosting();    // exactly next
        Posting GetPosting(const int &next_doc_ID);    // next Posting whose docID>next_doc_ID
// Add a doc for creating index
        void AddPosting(int docID, int term_frequency, const Positions positions);
// Serialize
        std::string serialize();
};


#endif
