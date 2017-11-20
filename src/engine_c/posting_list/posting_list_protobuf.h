#ifndef POSTING_LIST_PROTOBUF_H
#define POSTING_LIST_PROTOBUF_H

#include <string>
#include <string.h>
#include "engine_services.h"
#include "posting_basic.h"
#include "posting_message.pb.h"

// Posting_List Class
class Posting_List_Protobuf :  public PostingListStructure {
    std::string term;                    // term this posting list belongs to
    int num_postings;                    // number of postings in this list
    std::string serialized;              // serialized string reference, when query processing
    posting_message::PostingList plist_message; // posting list
    Posting * p_list;                    // posting list when creating index
    int cur_index;                       // last posting we have returned
    int cur_docID;                       // last docID we have returned

    public:
// Init with a term string, when creating index
    Posting_List_Protobuf(std::string term_in);    // do we need term?

// Init with a term string, and posting list string reference, when query processing
    Posting_List_Protobuf(std::string term_in, std::string & serialized_in);    // configuration

// Destructor
    ~Posting_List_Protobuf();

// Get next posting for query processing
    Posting * next();    // exactly next
    Posting * next(int next_doc_ID);    // next Posting whose docID>next_doc_ID

    void get_next_index();   // get to next index which is the starting point of a Posting
    void get_cur_DocID();    // get the DocID of current Posting which starts from cur_index
    Posting * get_next_Posting();  // read and parse the Posting which starts from cur_index, after this: cur_index is the end of this posting

// Add a doc for creating index
    void add_doc_new(std::string & doc_string);  // use protobuf
    void add_doc(int docID, int term_frequency, int position[]);  // TODO what info? who should provide?

// Serialize the posting list to store 
    std::string serialize();    // serialize the posting list, return a string
};

#endif
