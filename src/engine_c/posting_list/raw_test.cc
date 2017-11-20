#include <stdio.h>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include "Posting_List_Raw.h"

int terms[5][5] = {{1,2,3,4,5},
                   {6,7,8,9,10},
                   {1,3,5,7,9},
                   {2,4,6,8,10},
                   {1,2,3,5,8}
                   };

std::map<int, Posting_List *> dictionary;            // termID -> posting list
std::map<std::string, std::string> inverted_index;   // term -> posting lists(serialized)


int main(int argc, char* arg[]) {
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
                //dictionary[terms[i][j]] = Posting_List(std::to_string("hello") + std::to_string(terms[i][j]));
                std::string term = "term_" + std::to_string(terms[i][j]);
                dictionary[terms[i][j]] = new Posting_List(term);
            }
            // add_doc
            // create doc message
            /*posting_message::doc_info this_doc; 
            {
                this_doc->set_id(i);
                this_doc->set_term_frequency(term_frequency);
                posting_message::
            }*/
            // add doc
            dictionary[terms[i][j]]->add_doc(i, term_frequency, position); 
        }
    }
    

// store the inverted index(serialize)
    std::map<int, Posting_List*>::iterator it;
    for (it = dictionary.begin(); it != dictionary.end(); it++) {
        // serialize the posting list to a string 
        std::string serialized_result = it->second->serialize();
        inverted_index["term_"+std::to_string(it->first)] = serialized_result;
        // hint
        std::cout << "Posting list of term_" << it->first 
                   << ", after serialized: " <<  serialized_result << std::endl;
        // delete object
        delete it->second;
    }

// query (all terms)
    std::map<std::string, std::string>::iterator it_query;
    for (it_query = inverted_index.begin(); it_query != inverted_index.end(); it_query++) {
        // hint
        
        std::cout << "\n"<<it_query->first << " : " << it_query->second <<std::endl;
        // build iterator on postings of this term's posting list
        Posting_List * pl = new Posting_List(it_query->first, it_query->second);
        Posting * p = pl->next();
        while (p!=NULL) {
            std::cout<< "\nDump the posting: " << p->dump();
            p = pl->next();
        }
        std::cout<< "\nalready get to the end\n";
    }

    return 0;
}
