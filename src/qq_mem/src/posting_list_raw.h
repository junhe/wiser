#ifndef POSTING_LIST_RAW_H
#define POSTING_LIST_RAW_H

#include <string>
#include <string.h>
#include "engine_services.h"
#include "posting_basic.h"

// Posting_List Class
class PostingList_Raw :  public PostingListService {
    Term term_;                          // term this posting list belongs to
    std::vector<Posting> p_list;         // posting list when creating index
    
    int num_postings;                    // number of postings in this list
    int cur_index;                       // last posting we have returned
    int cur_docID;                       // last docID we have returned
    
    //TODO 
    std::string serialized;              // serialized string reference, when query processing

    public:

// Init with a term string, when creating index
        PostingList_Raw(std::string term_in);
// Init with a term string, and posting list string reference, when query processing
        PostingList_Raw(std::string term_in, std::string & serialized_in);    // configuration
// Return size
        std::size_t Size();
// Get next posting for query processing
        Posting GetPosting();    // exactly next
        Posting GetPosting(const int &next_doc_ID);    // next Posting whose docID>next_doc_ID
// Add a doc for creating index
        void AddPosting(int docID, int term_frequency, const Positions positions);
// Serialize
        std::string Serialize();

// helper function
    private:
        void get_next_index();         // get to next index which is the starting point of a Posting
        void get_cur_DocID();          // get the DocID of current Posting which starts from cur_index
        Posting get_next_Posting();  // read and parse the Posting which starts from cur_index, after this: cur_index is the end of this posting

};


#endif
