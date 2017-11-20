#ifndef POSTING_LIST_DIRECT_H
#define POSTING_LIST_DIRECT_H

#include <string>
#include <string.h>
#include <vector>
#include "engine_services.h"
#include "posting_basic.h"

// Posting_List Class
class Posting_List_Direct :  public PostingListStructure {
    std::string term;                    // term this posting list belongs to
    int num_postings;                    // number of postings in this list
    int cur_index;                       // last posting we have returned
    int cur_docID;                       // last docID we have returned

    public:
    std::vector<Posting *> p_list;         // posting list when creating index
// Init with a term string, when creating index
    Posting_List_Direct(std::string term_in);    // do we need term?

// Destructor
    ~Posting_List_Direct();

// Get next posting for query processing
    Posting * next();    // exactly next
    Posting * next(int next_doc_ID);    // next Posting whose docID>next_doc_ID

// Add a doc for creating index
    void add_doc(int docID, int term_frequency, int position[]);  // TODO what info? who should provide?
    std::string serialize() {}
};


#endif
