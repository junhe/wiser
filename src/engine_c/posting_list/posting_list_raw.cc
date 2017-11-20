#include "posting_list_raw.h"


// Posting_List_Raw Class
// Init with a term string, when creating index
    Posting_List_Raw::Posting_List_Raw(std::string term_in) {  // do we need term?
        term = term_in;
        num_postings = 0;
        p_list = NULL;
        serialized = "";
        cur_index = 0;
        cur_docID = -1;
    }

    Posting_List_Raw::~Posting_List_Raw() {
        // delete all Postings
        Posting * last_posting;
        while (p_list != NULL) {
            last_posting = p_list;
            p_list = p_list->next;
            delete last_posting;
        } 
    }

// Init with a term string, and posting list string reference, when query processing
    Posting_List_Raw::Posting_List_Raw(std::string term_in, std::string & serialized_in) {  // configuration
        term = term_in;
        serialized = serialized_in;
        p_list = NULL;
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

// Get next posting
    Posting * Posting_List_Raw::next() {  // exactly next
        return next(cur_docID+1);
    } 

    Posting * Posting_List_Raw::next(int next_doc_ID) { // next Posting whose docID>next_doc_ID
        //printf("\nbefore next, now cur_index: %d", cur_index);
        // get the string for next Posting
        while (cur_docID < next_doc_ID) {
            get_next_index();
            if (cur_index == -1)  // get the end
                return NULL;
            get_cur_DocID();
        }
        return get_next_Posting();
    }

    void Posting_List_Raw::get_next_index() {  // get to next index which is the starting point of a Posting
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

    void Posting_List_Raw::get_cur_DocID() {  // get the DocID of current Posting which starts from cur_index
        std::string tmp = "";
        int tmp_index = cur_index+1;
        while (serialized[tmp_index]!='_') {
            tmp += serialized[tmp_index];
            tmp_index++;
        }
        cur_docID = stoi(tmp);
    }

    Posting * Posting_List_Raw::get_next_Posting() {  // read and parse the Posting which starts from cur_index, after this: cur_index is the end of this posting
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
        int * tmp_positions = (int *) malloc(tmp_frequency * sizeof(int));
        int tmp_length = serialized.length();
        for (int i = 0; i < tmp_frequency; i++) {
            std::string tmp = "";
            while (tmp_index < tmp_length && serialized[tmp_index]!='_' && serialized[tmp_index]!='-') {
                tmp += serialized[tmp_index];
                tmp_index++;
            }
            tmp_index++;
            tmp_positions[i] = stoi(tmp);
        }

        // change cur_index
        cur_index = tmp_index-2;
        Posting * result = new Posting(cur_docID, tmp_frequency, tmp_positions);
        free(tmp_positions);
        return result;
    }

// Add a doc for creating index
    void Posting_List_Raw::add_doc(int docID, int term_frequency, int position[]) { // TODO what info? who should provide?
        //std::cout << "Add document " << docID << " for " << term << std::endl;
        // add one posting to the p_list
        num_postings += 1;
        if (p_list == NULL) {
            p_list = new Posting(docID, term_frequency, position);
        } else {  // insert into the posting list sorted by docID
            Posting * tmp_posting = p_list;
            Posting * tmp1_posting = NULL;
            while (tmp_posting != NULL) {
                if (tmp_posting->docID > docID)
                    break;
                tmp1_posting = tmp_posting;
                tmp_posting = tmp_posting->next;
            }
            Posting * tmp2_posting = new Posting(docID, term_frequency, position);
            tmp2_posting->next = tmp_posting;
            if (tmp1_posting != NULL) {
                tmp1_posting->next = tmp2_posting; 
            } else {
                p_list = tmp2_posting;
            }
        }

        return;
    }

// Serialize the posting list
    std::string Posting_List_Raw::serialize() {
        /* format:
        num_postings*[-docID_termfrequency_*[_position]]
        Eg. 3-0_5_1_10_20_31_44-2_5_1_10_20_31_44-4_5_1_10_20_31_44
        three postings
        */
        std::string result = std::to_string(num_postings);
        // traverse all postings
        Posting * cur_posting = p_list;
        while (cur_posting != NULL) {
            result += cur_posting->dump();
            cur_posting = cur_posting->next;
        }
        return result;
    }


