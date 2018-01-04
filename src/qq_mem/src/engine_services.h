#ifndef ENGINE_SERVICES_H
#define ENGINE_SERVICES_H


#include <string>
#include <vector>
#include <map>

typedef int DocIdType;
typedef std::string Term;
typedef std::vector<Term> TermList;
enum class SearchOperator {AND, OR};
// typedef float qq_float;
typedef double qq_float;

class DocumentStoreService {
    public:
        virtual void Add(int id, std::string document) = 0;
        virtual void Remove(int id) = 0;
        virtual std::string Get(int id) = 0;
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

#endif
