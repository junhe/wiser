#include <stdio.h>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include "Posting_List.h"

int terms[5][5] = {{1,2,3,4,5},
                   {6,7,8,9,10},
                   {1,3,5,7,9},
                   {2,4,6,8,10},
                   {1,2,3,5,8}
                   };

std::map<int, Posting_List *> dictionary;    // termID -> posting list


int main(int argc, char* arg[]) {
    printf("==== Get into test of posting list \n");

    int doc_amount = 5;
    
// construct inverted index
    for (int i = 0; i < doc_amount; i++) {
        // read in each document
        // get terms from that document (set to be a two dimensional matrix)
        // update posting list for each terms
        for (int j = 0; j < 5; j++) {
            int term_frequency = (i+1)*(j+1);
            int position[5] = {1, 10, 20, 31, 44};
            if (dictionary.find(terms[i][j]) == dictionary.end()) {
                //dictionary[terms[i][j]] = Posting_List(std::to_string("hello") + std::to_string(terms[i][j]));
                std::string term = "term_" + std::to_string(terms[i][j]);
                dictionary[terms[i][j]] = new Posting_List(term);
            }
            // addoc
            dictionary[terms[i][j]]->add_doc(i, term_frequency, 5, position); 
        }


    }
    

// store the inverted index(serialize)
    std::map<int, Posting_List*>::iterator it;
    for (it = dictionary.begin(); it != dictionary.end(); it++) {
        // serialize the posting list to a string 
        std::cout << "Posting list of term_" << it->first 
                   << ", after serialized: " <<  it->second->serialize() << std::endl;
        // clean the objects
    }

// query (just set to all terms)
    for (int i = 0; i < 5; i++) {
        // create a posting list for reading
         
    }

    return 0;
}
