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
        typedef std::unordered_map<int, FlashAddress> PostingFlashStore;

        Term term_;                             // term this posting list belongs to
        PostingStore posting_store_;            // for in-mem QQ, storing real postings
        PostingFlashStore posting_flash_store_; // for flash-based QQ, storing address of the postings on flash

    public:

        // Implement iterators, so multiple threads can iterate differently
        // Iterators should not be used directly. They should be passed to 
        // ExtractPosting() to get the posting.
        typedef PostingStore::iterator iterator;
        typedef PostingStore::const_iterator const_iterator;
        iterator begin() {return posting_store_.begin();}
        iterator end() {return posting_store_.end();}
        const_iterator cbegin() {return posting_store_.cbegin();}
        const_iterator cend() {return posting_store_.cend();}
        
        PostingList_Direct(std::string term_in);    // do we need term?
        std::size_t Size();

        // Add a doc for creating index
        void AddPosting(int docID, int term_frequency, const Offsets positions);
        Posting ExtractPosting(const_iterator it);
        // Find a posting according to docID
        Posting & GetPosting(const int & docID);
        // Serialize
        std::string Serialize();
};


#endif
