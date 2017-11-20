#include "posting_basic.h"
#include <string.h>


//Posting
    Posting::Posting(int docID_in, int term_frequency_in, int position_in[]) {
        docID = docID_in;
        term_frequency = term_frequency_in;
        position = (int *) malloc(sizeof(int)*term_frequency);
        memcpy(position, position_in, sizeof(int)*term_frequency); 
        next = NULL;
    }

    Posting::~Posting() {
        free(position);
    }

    std::string Posting::dump() {
        std::string result="-";
        result = result + std::to_string(docID)+ "_" + std::to_string(term_frequency);
        for (int i = 0; i < term_frequency; i++) {
            result += "_" + std::to_string(position[i]);
        }
        return result;
    }

