#include "posting_list_raw.h"
#include <iostream>

// Posting_List_Raw Class
// Init with a term string, when creating index
PostingList_Raw::PostingList_Raw(std::string term_in) {  // do we need term?
    term_ = term_in;
    num_postings = 0;
    p_list = {};
    serialized = "";
    cur_index = 0; // 0 TODO
    cur_docID = -1;
}

// Init with a term string, and posting list string reference, when query processing
PostingList_Raw::PostingList_Raw(std::string term_in, std::string & serialized_in) {  // configuration
    term_ = term_in;
    serialized = serialized_in;
    p_list = {};
    cur_index = 0;
    cur_docID = -1;
    // read posting list head(num_postings)
    std::string tmp = "";
    while (serialized[cur_index]!='-') {
        tmp += serialized[cur_index];
        cur_index++;
    }
    num_postings = stoi(tmp);
    cur_index--;
    // hint
    //std::cout<< "num of postings: " << num_postings << std::endl;
}

// Return size
std::size_t PostingList_Raw::Size() {
    return num_postings;
}

// Get next posting
Posting PostingList_Raw::GetPosting() {  // exactly next
    return GetPosting(cur_docID+1);
} 

Posting PostingList_Raw::GetPosting(const int &next_doc_ID) {   // next Posting whose docID>next_doc_ID
    // get the string for next Posting
    while (cur_docID < next_doc_ID) {
        get_next_index();
        if (cur_index == -1)  // get the end
            return Posting(-1, 0, {});
        get_cur_DocID();
    }
    return get_next_Posting();
}

void PostingList_Raw::get_next_index() {  // get to next index which is the starting point of a Posting
    if (cur_index == -1)
        return;
    cur_index++;
    while (cur_index < serialized.length() && serialized[cur_index]!='-') {
        cur_index++;
    }
    if (cur_index == serialized.length()) {
        cur_index = -1;
    }
}

void PostingList_Raw::get_cur_DocID() {  // get the DocID of current Posting which starts from cur_index
    std::string tmp = "";
    int tmp_index = cur_index+1;
    while (serialized[tmp_index]!='_') {
        tmp += serialized[tmp_index];
        tmp_index++;
    }
    cur_docID = stoi(tmp);
}

Posting PostingList_Raw::get_next_Posting() {  // read and parse the Posting which starts from cur_index, after this: cur_index is the end of this posting
    //printf("\nget here before next_posting: cur index:%d cur_DocID: %d", cur_index, cur_docID);
    // parse docID, here just skip those characters
    int tmp_index = cur_index;
    while (serialized[tmp_index]!='_') {
        tmp_index++;
    }

    tmp_index++;
    // parse term frequency
    std::string tmp = "";
    while (serialized[tmp_index]!='_') {
        tmp += serialized[tmp_index];
        tmp_index++;
    }

    int tmp_frequency = stoi(tmp);
    tmp_index++;

    // parse positions
    Offsets tmp_positions = {};
    int tmp_length = serialized.length();
    for (int i = 0; i < tmp_frequency; i++) {
        std::string tmp = "";
        while (tmp_index < tmp_length && serialized[tmp_index]!='_' && serialized[tmp_index]!='-') {
            tmp += serialized[tmp_index];
            tmp_index++;
        }
        tmp_index++;
        //TODO change from postion to offset
        //tmp_positions.push_back(stoi(tmp));
    }

    // change cur_index
    cur_index = tmp_index-2;
    Posting result(cur_docID, tmp_frequency, tmp_positions);
    return result;
}

// Add a doc for creating index
void PostingList_Raw::AddPosting(int docID, int term_frequency, const Offsets positions) {
    //std::cout << "Add document " << docID << " for " << term_ << ": " << std::to_string(positions[0]) << std::endl;
    // add one posting to the p_list
    num_postings += 1;
    Posting new_posting(docID, term_frequency, positions);
    p_list.push_back( new_posting );  //TODO
    return;
}

// Serialize the posting list
std::string PostingList_Raw::Serialize() {
    /* format:
    num_postings*[-docID_termfrequency_*[_position]]
    Eg. 3-0_5_1_10_20_31_44-2_5_1_10_20_31_44-4_5_1_10_20_31_44
    three postings
    */
    std::string result = std::to_string(num_postings);
    // traverse all postings
    for (int i = 0; i < num_postings; i++) { 
        result += p_list[i].dump();
    }
    return result;
}


