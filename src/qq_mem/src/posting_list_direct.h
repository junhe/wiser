#ifndef POSTING_LIST_DIRECT_H
#define POSTING_LIST_DIRECT_H

#include <string>
#include <string.h>
#include <vector>
#include <unordered_map>
#include "engine_services.h"
#include "posting_basic.h"



class PostingList_Direct :  public PostingListService {
    private:
        typedef std::unordered_map<int, Posting> PostingStore;

        Term term_;                          // term this posting list belongs to
        PostingStore posting_store_;         
    
    public:

        // Implement iterators
        typedef PostingStore::iterator iterator;
        typedef PostingStore::const_iterator const_iterator;
        iterator begin() {return posting_store_.begin();}
        iterator end() {return posting_store_.end();}
        const_iterator cbegin() {return posting_store_.cbegin();}
        const_iterator cend() {return posting_store_.cend();}
        
        PostingList_Direct(std::string term_in);    // do we need term?
        std::size_t Size();

        // Add a doc for creating index
        void AddPosting(int docID, int term_frequency, const Positions positions);
        // Serialize
        std::string Serialize();
};


#endif
