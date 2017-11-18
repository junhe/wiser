#include "posting_list.hh"
#include <iostream>

Posting::Posting(int docID_in, int term_frequency, int num_pos, int position[]) {
    docID = docID_in;
    next = NULL;
    int i = 0;
}

std::string Posting::dump() {
    return std::to_string(docID);
}

Posting_List::Posting_List(std::string term_in) {
    term = term_in;
    p_list = NULL;
}

void Posting_List::add_doc(int docID, int term_frequency, int num_pos, int position[]) {
    std::cout << "Add document " << docID << " for " << term << std::endl;
    // add one posting to the p_list
    if (p_list == NULL) {
        p_list = new Posting(docID, term_frequency, num_pos, position);
    } else {  // insert into the posting list sorted by docID
        Posting * tmp_posting = p_list;
        Posting * tmp1_posting = NULL;
        while (tmp_posting != NULL) {
            if (tmp_posting->docID > docID)
                break;
            tmp1_posting = tmp_posting;
            tmp_posting = tmp_posting->next;
        }
        Posting * tmp2_posting = new Posting(docID, term_frequency, num_pos, position);
        tmp2_posting->next = tmp_posting;
        if (tmp1_posting != NULL) {
            tmp1_posting->next = tmp2_posting; 
        } else {
            p_list = tmp2_posting;
        }
    }

    return;
}

std::string Posting_List::serialize() {
    std::string result = "";
    // traverse all postings
    Posting * cur_posting = p_list;
    while (p_list != NULL) {
        result += p_list->dump();
        p_list = p_list->next;
    }
    return result;
}



