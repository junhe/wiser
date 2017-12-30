#ifndef ENGINE_SERVICES_H
#define ENGINE_SERVICES_H


#include <string>
#include <vector>
#include <map>
#include <tuple>
#include "types.h"

// This class is the basic abstract class for
// different kinds of search engine implementations:
//   1. QQMemDirectSearchEngine: all contents directly(no serialization) stored in memory
//   2.
class SearchEngineService {
 public:
  // Generate unique id for each document
  int NextDocId();
  // Add one more document into this search engine document base
  void AddDocument(const std::string &title, const std::string &url, 
                   const std::string &body){}
  void AddDocument(const std::string &title, const std::string &url, 
                   const std::string &body, const std::string &tokens,
                   const std::string &offsets){} // with analyzed info(tokens, offsets) about this doc
  // Get document content according to unique doc_id
  const std::string & GetDocument(const int &doc_id){}
  // Search a query, get top-related document ids 
  std::vector<int> Search(const TermList &terms, const SearchOperator &op){}
  // Batch-load local documents into this search engine document base
  int LoadLocalDocuments(const std::string &line_doc_path, int n_rows){}
  
 private:
  int next_doc_id_ = 0;
};

class DocumentStoreService {
    public:
        virtual void Add(int id, std::string document) = 0;
        virtual void Remove(int id) = 0;
        virtual const std::string & Get(int id) = 0;
        virtual bool Has(int id) = 0;
        virtual void Clear() = 0;
        virtual int Size() = 0;
};

class InvertedIndexService {
    public:
        virtual void AddDocument(const int &doc_id, const TermList &termlist) = 0;
        virtual std::vector<int> GetDocumentIds(const Term &term) = 0;
        virtual std::vector<int> Search(const TermList &terms, const SearchOperator &op) = 0;
};

class PostingService {
    public:
        virtual std::string dump() = 0;
};


// Posting_List Class
class PostingListService {
    public:
// Get size
        virtual std::size_t Size() = 0;
// Get next posting for query processing
        // virtual PostingService GetPosting() = 0;                          // exactly next
        // virtual PostingService GetPosting(const int &next_doc_ID) = 0;    // next Posting whose docID>next_doc_ID
// Add a doc for creating index
        // virtual void AddPosting(int docID, int term_frequency, const Positions positions) = 0;
// Serialize the posting list to store 
        virtual std::string Serialize() = 0;    // serialize the posting list, return a string

};

class FlashReader {
    public:
        FlashReader(const std::string & based_file);
        ~FlashReader();
        std::string read(const Store_Segment & segment);
        Store_Segment append(const std::string & value);

    private:
        std::string _file_name_;
        int _fd_;
        int _last_offset_; // append from it
};

// TODO: this design is not good!
extern FlashReader * Global_Posting_Store; //defined in qq_engine.cc
extern FlashReader * Global_Document_Store; //defined in qq_engine.cc
#endif
