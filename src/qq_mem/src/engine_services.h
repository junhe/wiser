#ifndef ENGINE_SERVICES_H
#define ENGINE_SERVICES_H


#include <string>
#include <vector>
#include <map>

class DocumentStoreService {
    public:
        virtual void Add(int id, std::string document) = 0;
        virtual void Remove(int id) = 0;
        virtual std::string Get(int id) = 0;
        virtual bool Has(int id) = 0;
        virtual void Clear() = 0;
        virtual int Size() = 0;
};

typedef std::string Term;
typedef std::vector<Term> TermList;
enum class SearchOperator {AND, OR};

class InvertedIndexService {
    public:
        virtual void AddDocument(int doc_id, TermList termlist) = 0;
        virtual void GetPostingList(int doc_id) = 0;
        virtual void Search(TermList terms, SearchOperator op) = 0;
};

class PostingService {
    public:
        virtual std::string dump() = 0;
};

// Posting_List Class
class PostingListService {
    public:
// Get next posting for query processing
        virtual PostingStructure * next() = 0;                   // exactly next
        virtual PostingStructure * next(int next_doc_ID) = 0;    // next Posting whose docID>next_doc_ID
// Add a doc for creating index
        virtual void add_doc(int docID, int term_frequency, int position[]) = 0;  // TODO what info? who should provide?
// Serialize the posting list to store 
        virtual std::string serialize() = 0;    // serialize the posting list, return a string
};


#endif
