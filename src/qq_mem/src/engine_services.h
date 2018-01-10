#ifndef ENGINE_SERVICES_H
#define ENGINE_SERVICES_H


#include <string>
#include <vector>
#include <map>
#include <tuple>
#include "types.h"
#include <boost/locale.hpp>

// This class is the basic abstract for
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

class SearchEngineServiceNew {
 public:
  // tokenized_body is stemmed and tokenized. It may have repeated token. 
  virtual void AddDocument(const std::string &body, const std::string &tokenized_body) = 0;
  // virtual Snippets Search(const TermList &terms, const SearchOperator &op) = 0; 
  virtual SearchResult Search(const SearchQuery &query) = 0; 
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

struct DocScore {
  DocIdType doc_id;
  qq_float score;

  DocScore(const DocIdType &doc_id_in, const qq_float &score_in)
    :doc_id(doc_id_in), score(score_in) {}

  friend bool operator<(DocScore a, DocScore b)
  {
    return a.score < b.score;
  }

  std::string ToStr() {
    return std::to_string(doc_id) + " (" + std::to_string(score) + ")\n";
  }
};

typedef std::vector<DocScore> DocScoreVec;
typedef std::map<Term, qq_float> TermScoreMap;



class InvertedIndexService {
    public:
        virtual void AddDocument(const int &doc_id, const TermList &termlist) = 0;
        virtual std::vector<int> GetDocumentIds(const Term &term) = 0;
        virtual std::vector<int> Search(const TermList &terms, const SearchOperator &op) = 0;
};

class PostingService {
};

class QqMemPostingService {
 public:
  virtual const int GetDocId() const = 0;
  virtual const int GetTermFreq() const = 0;
  virtual const OffsetPairs *GetOffsetPairs() const = 0;
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
extern FlashReader * Global_Posting_Store;  //defined in qq_engine.cc
extern FlashReader * Global_Document_Store; //defined in qq_engine.cc
extern boost::locale::generator gen_all;        //
extern std::locale locale_all;        // all generator
#endif
