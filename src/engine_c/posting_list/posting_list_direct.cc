#include "posting_list_direct.h"
#include <iostream>
// Init with a term string, when creating index
    Posting_List_Direct::Posting_List_Direct(std::string term_in) {
        term = term_in;
        num_postings = 0;
        p_list = {};
        cur_index = -1;
        cur_docID = -1;
    }

    Posting_List_Direct::~Posting_List_Direct() {
    }

// Get next posting
    Posting * Posting_List_Direct::next() {  // exactly next
        return next(cur_docID+1);
    } 

    Posting * Posting_List_Direct::next(int next_doc_ID) { // next Posting whose docID>next_doc_ID
        while (cur_docID < next_doc_ID) {
            if (cur_index == num_postings-1)  // get the end
                return NULL;
            cur_index++;
            cur_docID = p_list[cur_index]->docID;
        }
        return p_list[cur_index];
    }


// Add a doc for creating index
    void Posting_List_Direct::add_doc(int docID, int term_frequency, int position[]) { // TODO what info? who should provide?
        std::cout << "Add document " << docID << " for " << term << std::endl;
        // add one posting to the p_list
        num_postings += 1;
        Posting * new_posting = new Posting(docID, term_frequency, position);
        p_list.push_back( new_posting );  //TODO
        return;
    }

