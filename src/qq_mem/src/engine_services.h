#ifndef ENGINE_SERVICES_HH
#define ENGINE_SERVICES_HH


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


class PostingListService {
};



typedef std::string Term;
typedef std::vector<Term> TermList;
enum class SearchOperator {AND, OR};

class InvertedIndexService {
    public:
        virtual void AddDocument(const int &doc_id, const TermList &termlist) = 0;
        virtual std::vector<int> GetDocumentIds(const Term &term) = 0;
        virtual void Search(const TermList &terms, const SearchOperator &op) = 0;
};

#endif

