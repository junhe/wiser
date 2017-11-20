#include <stdio.h>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include "posting_list_direct.h"

int terms[5][5] = {{1,2,3,4,5},
                   {6,7,8,9,10},
                   {1,3,5,7,9},
                   {2,4,6,8,10},
                   {1,2,3,5,8}
                   };

std::map<int, Posting_List_Direct *> dictionary;            // termID -> posting list
std::map<std::string, std::string> inverted_index;   // term -> posting lists(serialized)


int main(int argc, char* argv[]) {
    printf("==== Get into test of posting list \n");

    int doc_amount = 5;
    
// construct inverted index
    for (int i = 0; i < doc_amount; i++) {
        // read in each document
        // get terms from that document (set to be a two dimensional matrix)
        // update posting list for each terms
        for (int j = 0; j < 5; j++) {
            int term_frequency = 5;
            int position[5] = {1, 10, 20, 31, 44};
            if (dictionary.find(terms[i][j]) == dictionary.end()) {
                std::string term = "term_" + std::to_string(terms[i][j]);
                dictionary[terms[i][j]] = new Posting_List_Direct(term);
            }
            // add doc
            dictionary[terms[i][j]]->add_doc(i, term_frequency, position); 
            std::cout << dictionary[terms[i][j]]->p_list[0]->dump() << std::endl;
        }
    }
    

// store the inverted index(serialize)
    std::map<int, Posting_List_Direct*>::iterator it;
    for (it = dictionary.begin(); it != dictionary.end(); it++) {
        // build iterator on postings of this term's posting list
        std::cout<< it->first << std::endl;
        Posting_List_Direct * pl = it->second;
        Posting * p = pl->next();
        while (p!=NULL) {
            std::cout<< "\nDump the posting: " << p->dump() << std::endl;
            p = pl->next();
        }
        delete pl;
    }


    return 0;
}
