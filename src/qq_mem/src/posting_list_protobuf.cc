#include "posting_list_protobuf.h"
#include <iostream>

// Posting_List_Raw Class
// Init with a term string, when creating index
PostingList_Protobuf::PostingList_Protobuf(std::string term_in) {  // do we need term?
    term_ = term_in;
    num_postings = 0;
    p_list = {};
    serialized = "";
    cur_index = 0; // 0 TODO
    cur_docID = -1;
}

// Init with a term string, and posting list string reference, when query processing
PostingList_Protobuf::PostingList_Protobuf(std::string term_in, std::string & serialized_in) {  // configuration
    term_ = term_in;
    serialized = serialized_in;
    p_list = {};
    cur_index = -1;
    cur_docID = -1;
    // parse string and read posting list head(num_postings)
    plist_message.ParseFromString(serialized_in);
    num_postings = plist_message.num_postings();
    // hint
    //std::cout<< "num of postings: " << num_postings << std::endl;
}

// Return size
std::size_t PostingList_Protobuf::Size() {
    return num_postings;
}

// Get next posting
Posting PostingList_Protobuf::GetPosting() {  // exactly next
    return GetPosting(cur_docID+1);
} 

Posting PostingList_Protobuf::GetPosting(const int &next_doc_ID) {   // next Posting whose docID>next_doc_ID
    // get the string for next Posting
    while (cur_docID < next_doc_ID) {
        get_next_index();
        if (cur_index == -1)  // get the end
            return Posting(-1, 0, {});
        get_cur_DocID();
    }
    return get_next_Posting();
}

void PostingList_Protobuf::get_next_index() {  // get to next index which is the starting point of a Posting
    if (cur_index == num_postings)
        return;
    cur_index++;
}

void PostingList_Protobuf::get_cur_DocID() {  // get the DocID of current Posting which starts from cur_index
    cur_docID = plist_message.postings(cur_index).docid();
}

Posting PostingList_Protobuf::get_next_Posting() {  // read and parse the Posting which starts from cur_index, after this: cur_index is the end of this posting
    //printf("\nget here before next_posting: cur index:%d cur_DocID: %d", cur_index, cur_docID);
    // parse docID, here just skip those characters
    posting_message::Posting cur_posting = plist_message.postings(cur_index);
    int tmp_frequency = cur_posting.term_frequency();
    Offsets tmp_positions = {};
    for (int i = 0; i < tmp_frequency; i++)
        ;
        //TODO change from postion to offset
        //tmp_positions.push_back(cur_posting.positions(i));
    Posting result(cur_posting.docid(), tmp_frequency, tmp_positions);
    return result;
}

// Add a doc for creating index
void PostingList_Protobuf::AddPosting(int docID, int term_frequency, const Offsets positions) {
    //std::cout << "Add document " << docID << " for " << term_ << ": " << std::to_string(positions[0]) << std::endl;
    // add one posting to the p_list
    num_postings += 1;
    Posting new_posting(docID, term_frequency, positions);
    p_list.push_back( new_posting );  //TODO
    return;
}

// Serialize the posting list
std::string PostingList_Protobuf::Serialize() {
    /* format: protobuf message in posting_message.proto
    */
    posting_message::PostingList pl_message;
    {
        // header: num_postings 
        pl_message.set_num_postings(num_postings);
        // postings:
        for (int i = 0; i < num_postings; i++) { 
            posting_message::Posting * new_posting = pl_message.add_postings();
            new_posting->set_docid(p_list[i].docID_);
            new_posting->set_term_frequency(p_list[i].term_frequency_);
            for (int k = 0; k < p_list[i].term_frequency_; k++) ;
                // TODO change from positions to offset new_posting->add_positions(p_list[i].positions_[k]);
        }
    }
    std::string pl_string;
    pl_message.SerializeToString(&pl_string);
    return pl_string;
}


